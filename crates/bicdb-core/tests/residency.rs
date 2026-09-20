//! Resident-memory accounting (Phase 0 of `docs/server-paged-storage-todo.md`).
//!
//! These assert the properties that make the numbers *usable*, not exact byte
//! counts — the estimates are documented as approximate and pinning them would
//! make the suite fail on every allocator or layout change without catching a
//! single real regression. What must hold is that the accounting responds to
//! data the way the storage engine does: it grows with rows, it separates
//! reclaimable history from live rows, it attributes index and vector memory to
//! the right category, and it never double-counts a record shared by `Arc`
//! between the live map and its version chain.

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;
use tempfile::TempDir;

fn record(id: &str, payload: &str) -> Record {
    Record::new(id).with_metadata(json!({ "body": payload, "kind": "note" }))
}

/// Pinned to `embedded_memory`: this file asserts that resident accounting
/// tracks the *data* — rows_bytes growing with payload, history separated from
/// live rows. In paged mode rows are deliberately NOT resident (eviction
/// stubs), so those properties hold of the page store instead and are covered
/// by the storage-conformance suite's cross-mode residency assertion.
fn open(dir: &TempDir) -> BicDb {
    BicDb::open_with_config(
        dir.path(),
        DbConfig::default().with_storage_mode(StorageMode::EmbeddedMemory),
    )
    .unwrap()
}

fn kind_index() -> IndexDefinition {
    IndexDefinition {
        name: "notes_kind".to_string(),
        collection: "notes".to_string(),
        fields: vec![IndexField::MetadataPath(vec!["kind".to_string()])],
        kind: IndexKind::BTree,
        unique: false,
        predicate: None,
        exclusion: None,
    }
}

#[test]
fn empty_database_accounts_almost_nothing() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let report = db.residency_report().unwrap();

    assert_eq!(report.record_count, 0);
    assert_eq!(report.rows_bytes, 0);
    assert_eq!(report.version_chains_bytes, 0);
    assert_eq!(report.accounted_bytes, 0);
}

#[test]
// Pinned to embedded_memory: asserts segment/dirty-set/residency mechanics
// that do not exist in paged mode (segments carry no rows there). The paged
// sweep (BICDB_STORAGE_MODE=server_paged) covers that engine's own contract.
fn row_bytes_track_the_data_actually_stored() {
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir);
    db.create_collection("notes").unwrap();

    let baseline = db.residency_report().unwrap().rows_bytes;

    for i in 0..100 {
        db.insert("notes", record(&format!("n{i}"), &"x".repeat(1_000)))
            .unwrap();
    }
    let report = db.residency_report().unwrap();

    assert_eq!(report.record_count, 100);
    // 100 rows of ~1 KB metadata each must show up as at least that much.
    assert!(
        report.rows_bytes >= baseline + 100_000,
        "rows_bytes {} did not reflect ~100 KB of stored metadata",
        report.rows_bytes
    );
    assert_eq!(
        report.collections.len(),
        1,
        "per-collection breakdown should name the one collection"
    );
    assert_eq!(report.collections[0].name, "notes");
    assert_eq!(report.collections[0].record_count, 100);
}

#[test]
// Pinned to embedded_memory: asserts segment/dirty-set/residency mechanics
// that do not exist in paged mode (segments carry no rows there). The paged
// sweep (BICDB_STORAGE_MODE=server_paged) covers that engine's own contract.
fn rows_and_version_chains_do_not_double_count_shared_records() {
    // A committed record is one allocation shared by Arc between the live map
    // and its version chain. Summing both naively would roughly double every
    // row, which would make the whole memory budget wrong in the same direction
    // as the data grows — the worst kind of accounting bug.
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir);
    db.create_collection("notes").unwrap();

    let body = "y".repeat(10_000);
    for i in 0..20 {
        db.insert("notes", record(&format!("n{i}"), &body)).unwrap();
    }

    let report = db.residency_report().unwrap();
    let logical = 20 * body.len() as u64;

    // Live payload is ~200 KB. Allowing generous room for per-record overhead,
    // anything near 2x logical would mean each record was counted twice.
    assert!(
        report.rows_bytes + report.version_chains_bytes < logical * 2,
        "rows {} + chains {} looks like double counting of {} logical bytes",
        report.rows_bytes,
        report.version_chains_bytes,
        logical
    );
    assert!(report.rows_bytes >= logical);
}

#[test]
fn retained_history_is_attributed_to_version_chains_not_rows() {
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir);
    db.create_collection("notes").unwrap();

    let body = "z".repeat(4_000);
    db.insert("notes", record("n1", &body)).unwrap();
    let after_insert = db.residency_report().unwrap();

    // Overwrite the same key repeatedly. Live row count stays at one; every
    // superseded version is retained history.
    for i in 0..10 {
        db.insert("notes", record("n1", &format!("{body}{i}")))
            .unwrap();
    }
    let after_updates = db.residency_report().unwrap();

    assert_eq!(
        after_updates.record_count, 1,
        "overwrites must not increase the live row count"
    );
    assert!(
        after_updates.version_count > after_insert.version_count,
        "retained versions should have grown: {} -> {}",
        after_insert.version_count,
        after_updates.version_count
    );
    assert!(
        after_updates.version_chains_bytes > after_insert.version_chains_bytes,
        "history bytes should have grown with retained versions"
    );
    // The growth belongs to history, not to live rows.
    assert!(
        after_updates.version_chains_bytes > after_updates.rows_bytes,
        "10 retained versions of a 4 KB row should outweigh the single live row: \
         chains {} vs rows {}",
        after_updates.version_chains_bytes,
        after_updates.rows_bytes
    );
}

#[test]
fn secondary_index_memory_is_reported_separately_from_rows() {
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir);
    db.create_collection("notes").unwrap();
    for i in 0..200 {
        db.insert("notes", record(&format!("n{i:04}"), "small"))
            .unwrap();
    }

    let before = db.residency_report().unwrap();
    assert_eq!(before.secondary_indexes_bytes, 0);

    db.create_index(kind_index()).unwrap();

    let after = db.residency_report().unwrap();
    assert!(
        after.secondary_indexes_bytes > 0,
        "an index over 200 rows must account for some memory"
    );
    assert_eq!(after.indexes.len(), 1);
    assert_eq!(after.indexes[0].name, "notes_kind");
    assert_eq!(after.indexes[0].collection, "notes");
    assert_eq!(after.indexes[0].entry_count, 200);
    // Index memory is its own category, not folded into row memory.
    assert_eq!(after.rows_bytes, before.rows_bytes);
    assert_eq!(
        after.accounted_bytes,
        before.accounted_bytes + after.secondary_indexes_bytes
    );
}

#[test]
fn exact_vectors_are_reported_separately_from_row_metadata() {
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir);
    db.create_collection("embeds").unwrap();

    for i in 0..50 {
        db.insert(
            "embeds",
            Record::new(format!("e{i}"))
                .with_metadata(json!({ "n": i }))
                .with_vector(vec![0.25_f32; 128]),
        )
        .unwrap();
    }

    let report = db.residency_report().unwrap();
    // 50 vectors x 128 f32 = 25,600 bytes of vector payload, held once in the
    // rows and again in the denormalized exact-search store.
    assert!(
        report.exact_vectors_bytes >= 25_600,
        "exact vector store should hold at least the raw vector bytes, got {}",
        report.exact_vectors_bytes
    );
    assert_eq!(
        report.collections[0].exact_vectors_bytes,
        report.exact_vectors_bytes
    );
}

#[test]
fn accounted_bytes_is_the_sum_of_its_categories() {
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir);
    db.create_collection("notes").unwrap();
    for i in 0..25 {
        db.insert("notes", record(&format!("n{i}"), "body"))
            .unwrap();
    }
    db.create_index(kind_index()).unwrap();

    let report = db.residency_report().unwrap();
    let sum = report.rows_bytes
        + report.version_chains_bytes
        + report.primary_key_maps_bytes
        + report.secondary_indexes_bytes
        + report.exact_vectors_bytes
        + report.hnsw_bytes
        + report.graphs_bytes;
    assert_eq!(report.accounted_bytes, sum);

    // Per-collection totals must reconcile with the aggregate too.
    let per_collection: u64 = report.collections.iter().map(|c| c.total_bytes()).sum();
    assert_eq!(
        per_collection,
        report.rows_bytes
            + report.version_chains_bytes
            + report.primary_key_maps_bytes
            + report.exact_vectors_bytes
    );
}

#[test]
fn accounting_is_a_lower_bound_on_process_rss() {
    // The report claims `accounted_bytes` under-reports RSS and exposes the gap.
    // If accounting ever exceeded RSS the estimate would be structurally wrong
    // (counting bytes that are not resident), so the direction is worth pinning
    // even though the magnitude is not.
    let dir = TempDir::new().unwrap();
    let mut db = open(&dir);
    db.create_collection("notes").unwrap();
    for i in 0..500 {
        db.insert("notes", record(&format!("n{i}"), &"q".repeat(200)))
            .unwrap();
    }

    let report = db.residency_report().unwrap();
    if let Some(rss) = report.process_resident_bytes {
        assert!(rss > 0);
        assert!(
            report.accounted_bytes < rss,
            "accounted {} should not exceed process RSS {}",
            report.accounted_bytes,
            rss
        );
        assert_eq!(
            report.unaccounted_bytes,
            Some(rss as i64 - report.accounted_bytes as i64)
        );
    }
}

#[test]
// Pinned to embedded_memory: asserts segment/dirty-set/residency mechanics
// that do not exist in paged mode (segments carry no rows there). The paged
// sweep (BICDB_STORAGE_MODE=server_paged) covers that engine's own contract.
fn report_survives_reopen_and_reflects_rebuilt_state() {
    // Open reconstructs the resident projection from segments. The accounting
    // must describe the rebuilt heap, not a stale or empty one.
    let dir = TempDir::new().unwrap();
    {
        let mut db = open(&dir);
        db.create_collection("notes").unwrap();
        for i in 0..100 {
            db.insert("notes", record(&format!("n{i}"), &"w".repeat(500)))
                .unwrap();
        }
        db.close().unwrap();
    }

    let db = open(&dir);
    let report = db.residency_report().unwrap();
    assert_eq!(report.record_count, 100);
    assert!(report.rows_bytes >= 50_000);
    assert!(report.primary_key_maps_bytes > 0);
}
