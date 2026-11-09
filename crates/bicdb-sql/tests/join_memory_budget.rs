//! A small fixture reproduces the missing pre-allocation join guard without OOM.
use bicdb_core::{query_budget::QueryMemoryScope, BicDb, DbConfig, StorageMode};
use bicdb_sql::SqlSession;

#[test]
fn wide_join_stops_within_the_envelope_before_materializing_the_product() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut session = SqlSession::new_unprivileged(&mut db, "join_reader");
    let limit = 256 * 1024;
    {
        let memory = QueryMemoryScope::new(limit);
        let result = session.execute(
            "SELECT a.payload, b.payload FROM \
            (SELECT i, repeat('x',2048) AS payload FROM generate_series(1,30) AS i) a \
            CROSS JOIN (SELECT j, repeat('y',2048) AS payload FROM generate_series(1,30) AS j) b",
        );
        assert!(
            result.is_err(),
            "the join must refuse its oversized materialization"
        );
        assert_eq!(result.unwrap_err().sqlstate(), "53200");
        assert!(
            memory.peak_bytes() <= limit + 16 * 1024,
            "guard ran after excessive allocation: {} bytes for {limit} budget",
            memory.peak_bytes()
        );
    }
    // Resource rejection must release statement-local state and keep the handle usable.
    assert_eq!(session.execute("SELECT 1").unwrap().rows.len(), 1);
}
