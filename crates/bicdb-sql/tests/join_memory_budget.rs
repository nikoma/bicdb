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

#[test]
fn hash_outer_and_lateral_materializations_obey_the_same_envelope() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut session = SqlSession::new_unprivileged(&mut db, "join_reader");
    let queries = [
        "SELECT a.payload,b.payload FROM \
         (SELECT i%1 AS k,repeat('x',2048) AS payload FROM generate_series(1,30) AS i) a \
         JOIN (SELECT j%1 AS k,repeat('y',2048) AS payload FROM generate_series(1,30) AS j) b ON a.k=b.k",
        "SELECT a.payload,b.payload FROM \
         (SELECT i,repeat('x',2048) AS payload FROM generate_series(1,80) AS i) a \
         FULL JOIN (SELECT j,repeat('y',2048) AS payload FROM generate_series(1,80) AS j) b ON a.i+b.j<0",
        "SELECT a.payload,b.payload FROM \
         (SELECT i,repeat('x',2048) AS payload FROM generate_series(1,30) AS i) a \
         CROSS JOIN LATERAL (SELECT repeat('y',2048) AS payload FROM generate_series(1,30) AS j WHERE a.i>0) b",
    ];
    for sql in queries {
        let limit = 256 * 1024;
        {
            let memory = QueryMemoryScope::new(limit);
            let error = session.execute(sql).expect_err(sql);
            assert_eq!(error.sqlstate(), "53200", "{sql}: {error}");
            assert!(
                memory.peak_bytes() <= limit + 16 * 1024,
                "{sql}: late guard {}",
                memory.peak_bytes()
            );
        }
        assert_eq!(session.execute("SELECT 1").unwrap().rows.len(), 1);
    }
}

#[test]
fn direct_engine_execution_also_installs_a_budget_scope() {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let memory = QueryMemoryScope::new(64 * 1024);
    let result = bicdb_sql::SqlEngine::new(&db).execute(
        "SELECT a.i,b.j FROM \
        generate_series(1,100) AS a(i) CROSS JOIN generate_series(1,100) AS b(j)",
    );
    assert_eq!(result.unwrap_err().sqlstate(), "53200");
    assert!(memory.peak_bytes() <= 64 * 1024 + 4096);
}
