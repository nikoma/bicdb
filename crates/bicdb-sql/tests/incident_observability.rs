//! Memory attribution and index-size metrics — the two observability gaps
//! the crawl-ingestion incident could not answer.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn session_db(mode: StorageMode) -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode),
    )
    .unwrap();
    (dir, db)
}

fn json_of(sql: &mut SqlSession, query: &str) -> serde_json::Value {
    let SqlValue::String(text) = sql.execute(query).unwrap().rows[0][0].clone() else {
        panic!("expected JSON text from {query}");
    };
    serde_json::from_str(&text).unwrap()
}

#[test]
fn memory_report_attributes_rows_and_indexes() {
    let (_dir, mut db) = session_db(StorageMode::EmbeddedMemory);
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE landings (id TEXT PRIMARY KEY, src TEXT UNIQUE, payload TEXT)")
        .unwrap();
    let filler = "x".repeat(400);
    for chunk in (0..2_000).collect::<Vec<_>>().chunks(250) {
        let values: Vec<String> = chunk
            .iter()
            .map(|i| format!("('id-{i:06}', 'src-{i:06}', '{filler}')"))
            .collect();
        sql.execute(&format!(
            "INSERT INTO landings VALUES {}",
            values.join(", ")
        ))
        .unwrap();
    }

    let report = json_of(&mut sql, "SELECT bicdb_memory_report()");

    // Rows are attributed, and the collection appears with a credible size.
    assert!(report["rows_bytes"].as_u64().unwrap() > 0, "{report}");
    assert!(report["accounted_bytes"].as_u64().unwrap() > 0);
    let collections = report["collections"].as_array().unwrap();
    let landings = collections
        .iter()
        .find(|c| c["name"] == "landings")
        .expect("the collection must be attributed");
    assert_eq!(landings["record_count"].as_u64().unwrap(), 2_000);
    assert!(
        landings["rows_bytes"].as_u64().unwrap() > 2_000 * 400,
        "row bytes must at least cover the payloads: {landings}"
    );

    // Indexes are attributed individually with NON-ZERO sizes — the
    // `size_bytes: 0` defect the incident hit.
    let indexes = report["indexes"].as_array().unwrap();
    assert!(!indexes.is_empty(), "indexes must be attributed");
    let sized = indexes
        .iter()
        .filter(|index| index["store_bytes"].as_u64().unwrap_or(0) > 0)
        .count();
    assert!(
        sized > 0,
        "at least one index must report non-zero bytes: {indexes:?}"
    );

    // The unaccounted line exists so the gap is explicit rather than implied.
    assert!(report.get("unaccounted_bytes").is_some());
}

/// The regression itself: an index CREATED BEFORE its rows land (the restore
/// / schema-first order) used to record size_bytes 0 and never revisit it.
#[test]
fn index_size_refreshes_after_rows_land() {
    let (dir, mut db) = session_db(StorageMode::EmbeddedMemory);
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE t (id TEXT PRIMARY KEY, k TEXT)")
            .unwrap();
        // Index first, rows after — this is what pinned the value at 0.
        sql.execute("CREATE INDEX t_k ON t (k)").unwrap();
        let values: Vec<String> = (0..1_000)
            .map(|i| format!("('id-{i:05}', 'key-{i:05}')"))
            .collect();
        sql.execute(&format!("INSERT INTO t VALUES {}", values.join(", ")))
            .unwrap();
    }

    let refreshed = db.refresh_index_maintenance_sizes().unwrap();
    assert!(refreshed > 0, "the stale zero must be refreshed");

    let text = std::fs::read_to_string(dir.path().join("index-maintenance.json")).unwrap();
    let catalog: serde_json::Value = serde_json::from_str(&text).unwrap();
    let entry = &catalog["indexes"]["t_k"];
    assert!(
        entry["size_bytes"].as_u64().unwrap() > 0,
        "index-maintenance.json must report a real size, got {entry}"
    );
}
