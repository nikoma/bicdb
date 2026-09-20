//! L2: the space observability surface.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn space_report_accounts_for_files_and_paged_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    let values: Vec<String> = (0..500)
        .map(|index| {
            format!("('k{index:04}', 'payload {index} zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz')")
        })
        .collect();
    sql.execute(&format!("INSERT INTO docs VALUES {}", values.join(", ")))
        .unwrap();
    sql.execute("DELETE FROM docs WHERE id < 'k0250'").unwrap();

    let SqlValue::String(report) =
        sql.execute("SELECT bicdb_space_report()").unwrap().rows[0][0].clone()
    else {
        panic!("expected JSON");
    };
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();

    let total = report["total_file_bytes"].as_u64().unwrap();
    assert!(total > 0);
    // Categories sum to the total.
    let sum: u64 = report["categories"]
        .as_object()
        .unwrap()
        .values()
        .map(|value| value.as_u64().unwrap())
        .sum();
    assert_eq!(sum, total);
    assert!(report["categories"]["paged_store"].as_u64().unwrap() > 0);

    let paged = &report["paged"];
    assert!(paged["page_count"].as_u64().unwrap() > 0);
    assert_eq!(paged["page_size"].as_u64().unwrap() % 1024, 0);
    let logical = paged["logical_page_bytes"].as_u64().unwrap();
    assert_eq!(
        logical,
        paged["page_count"].as_u64().unwrap() * paged["page_size"].as_u64().unwrap()
    );
    // The reclaimable estimate is present and consistent.
    let reclaimable = paged["reclaimable_estimate_bytes"].as_u64().unwrap();
    let free = paged["free_bytes"].as_u64().unwrap();
    assert!(reclaimable >= free);

    // After VACUUM, the report still parses and reclaimable never grows.
    sql.execute("VACUUM").unwrap();
    let SqlValue::String(after) =
        sql.execute("SELECT bicdb_space_report()").unwrap().rows[0][0].clone()
    else {
        panic!("expected JSON");
    };
    let after: serde_json::Value = serde_json::from_str(&after).unwrap();
    assert!(after["paged"]["page_count"].as_u64().unwrap() > 0);
}
