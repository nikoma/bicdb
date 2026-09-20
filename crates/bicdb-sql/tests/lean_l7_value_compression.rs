//! L7: opt-in zstd value compression — write-new-read-both, big ratio win
//! on JSON-heavy metadata, and flipping the flag never needs a rebuild.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn config(compress: bool) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_value_compression(compress)
}

fn fill(sql: &mut SqlSession) {
    sql.execute("CREATE TABLE docs (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    // Highly compressible JSON-ish payloads, ~4KB each.
    let payload = "the quick brown fox jumps over the lazy dog ".repeat(90);
    for chunk in (0..300i64).collect::<Vec<_>>().chunks(50) {
        let values: Vec<String> = chunk
            .iter()
            .map(|index| format!("('k{index:04}', '{payload} #{index}')"))
            .collect();
        sql.execute(&format!("INSERT INTO docs VALUES {}", values.join(", ")))
            .unwrap();
    }
}

fn used_pages(sql: &mut SqlSession) -> u64 {
    let SqlValue::String(report) =
        sql.execute("SELECT bicdb_space_report()").unwrap().rows[0][0].clone()
    else {
        panic!("expected JSON");
    };
    let report: serde_json::Value = serde_json::from_str(&report).unwrap();
    report["paged"]["used_data_pages"].as_u64().unwrap()
}

#[test]
fn compression_shrinks_storage_and_round_trips() {
    let plain_dir = tempfile::tempdir().unwrap();
    let compressed_dir = tempfile::tempdir().unwrap();

    let mut plain = BicDb::open_with_config(plain_dir.path(), config(false)).unwrap();
    let mut sql = SqlSession::new(&mut plain);
    fill(&mut sql);
    let plain_pages = used_pages(&mut sql);
    drop(sql);

    let mut compressed = BicDb::open_with_config(compressed_dir.path(), config(true)).unwrap();
    let mut sql = SqlSession::new(&mut compressed);
    fill(&mut sql);
    let compressed_pages = used_pages(&mut sql);

    assert!(
        compressed_pages * 2 < plain_pages,
        "compressible payloads must at least halve used pages: {plain_pages} -> {compressed_pages}"
    );

    // Values round-trip exactly.
    let row = sql
        .execute("SELECT body FROM docs WHERE id = 'k0042'")
        .unwrap();
    let SqlValue::String(body) = &row.rows[0][0] else {
        panic!("expected text");
    };
    assert!(body.ends_with("#42"));
    assert_eq!(
        body.len(),
        "the quick brown fox jumps over the lazy dog ".len() * 90 + 4
    );
    drop(sql);

    // Read-both: reopen the compressed store WITHOUT compression — every
    // row still reads, and new writes are plain.
    compressed.close().unwrap();
    let mut reopened = BicDb::open_with_config(compressed_dir.path(), config(false)).unwrap();
    let mut sql = SqlSession::new(&mut reopened);
    let rows = sql.execute("SELECT count(*) FROM docs").unwrap();
    assert_eq!(rows.rows[0][0], SqlValue::Int(300));
    sql.execute("UPDATE docs SET body = 'short now' WHERE id = 'k0001'")
        .unwrap();
    let row = sql
        .execute("SELECT body FROM docs WHERE id = 'k0001'")
        .unwrap();
    assert_eq!(row.rows[0][0], SqlValue::String("short now".to_string()));

    // And flipping compression back on keeps reading the mixed store.
    drop(sql);
    reopened.close().unwrap();
    let mut mixed = BicDb::open_with_config(compressed_dir.path(), config(true)).unwrap();
    let mut sql = SqlSession::new(&mut mixed);
    let rows = sql.execute("SELECT count(*) FROM docs").unwrap();
    assert_eq!(rows.rows[0][0], SqlValue::Int(300));
}
