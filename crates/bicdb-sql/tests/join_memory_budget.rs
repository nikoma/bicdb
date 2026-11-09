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

fn guarded(session: &mut SqlSession<'_>, sql: &str, limit: usize) {
    {
        let memory = QueryMemoryScope::new(limit);
        let error = session
            .execute(sql)
            .expect_err("wide retained join rows must be bounded");
        assert_eq!(error.sqlstate(), "53200", "{sql}: {error}");
        assert!(
            memory.peak_bytes() <= limit + 64 * 1024,
            "guard ran after the whole product was retained: {} for {limit}: {sql}",
            memory.peak_bytes()
        );
    }
    assert_eq!(session.execute("SELECT 1").unwrap().rows.len(), 1);
}

#[test]
fn reused_wide_primary_lookup_payload_is_bounded_before_copying() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE jl(id INT PRIMARY KEY, fk INT); CREATE TABLE jr(id INT PRIMARY KEY,payload TEXT); \
        INSERT INTO jr VALUES(1,repeat('x',4096)); INSERT INTO jl SELECT i,1 FROM generate_series(1,100) AS i").unwrap();
    guarded(
        &mut s,
        "SELECT l.id,r.payload FROM jl l JOIN jr r ON l.fk=r.id",
        256 * 1024,
    );
}

#[test]
fn spatial_fanout_is_bounded_before_cloning_the_concatenated_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute(
        "CREATE TABLE ga(id INT PRIMARY KEY,geom TEXT,payload TEXT); \
        CREATE TABLE gb(id INT PRIMARY KEY,geom TEXT,payload TEXT); \
        INSERT INTO ga SELECT i,'POINT (0 0)',repeat('x',2048) FROM generate_series(1,12) AS i; \
        INSERT INTO gb SELECT i,'POINT (0 0)',repeat('y',2048) FROM generate_series(1,12) AS i",
    )
    .unwrap();
    guarded(
        &mut s,
        "SELECT a.payload,b.payload FROM ga a JOIN gb b ON ST_Intersects(a.geom,b.geom)",
        256 * 1024,
    );
}

#[test]
fn delete_using_candidate_rows_are_bounded_and_no_rows_are_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE dt(id INT PRIMARY KEY,fk INT); CREATE INDEX dt_fk ON dt(fk); \
        CREATE TABLE du(id INT PRIMARY KEY,payload TEXT); INSERT INTO du VALUES(1,repeat('x',4096)); \
        INSERT INTO dt SELECT i,1 FROM generate_series(1,100) AS i").unwrap();
    guarded(
        &mut s,
        "DELETE FROM dt USING du WHERE dt.fk=du.id AND length(du.payload)>0 RETURNING dt.id",
        256 * 1024,
    );
    assert_eq!(
        s.execute("SELECT count(*) FROM dt").unwrap().rows,
        vec![vec![bicdb_sql::SqlValue::Int(100)]]
    );
}

#[test]
fn indexed_update_from_candidates_share_the_materialization_budget() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut s = SqlSession::new(&mut db);
    s.execute("CREATE TABLE ut(id INT PRIMARY KEY,val INT); INSERT INTO ut SELECT i,0 FROM generate_series(1,70) AS i").unwrap();
    guarded(
        &mut s,
        "UPDATE ut SET val=length(u.payload) FROM \
        (SELECT i AS id,repeat('x',4096) AS payload FROM generate_series(1,70) AS i) u \
        WHERE ut.id=u.id RETURNING ut.id",
        512 * 1024,
    );
    assert_eq!(
        s.execute("SELECT sum(val) FROM ut").unwrap().rows,
        vec![vec![bicdb_sql::SqlValue::Int(0)]]
    );
}
