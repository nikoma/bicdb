//! Indexes and constraints of a partitioned table reach its partitions as in
//! PostgreSQL: CREATE INDEX builds one per partition (recursively), new and
//! attached partitions get theirs, an attached index cannot be dropped alone,
//! and a UNIQUE index must include the partition key.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn s(value: &str) -> SqlValue {
    SqlValue::String(value.into())
}

fn rows(sql: &mut SqlSession<'_>, query: &str) -> Vec<Vec<SqlValue>> {
    sql.execute(query).unwrap().rows
}

fn partition_indexes(sql: &mut SqlSession<'_>, pattern: &str) -> Vec<Vec<SqlValue>> {
    rows(
        sql,
        &format!(
            "SELECT t.relname, c.relname, c.relkind FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
             JOIN pg_class t ON t.oid = i.indrelid WHERE t.relname LIKE '{pattern}' ORDER BY 1, 2"
        ),
    )
}

#[test]
fn create_index_on_partitioned_table_reaches_every_partition() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE pi (a int, b int) PARTITION BY RANGE (a)")
            .unwrap();
        sql.execute("CREATE TABLE pi_1 PARTITION OF pi FOR VALUES FROM (0) TO (10)")
            .unwrap();
        sql.execute(
            "CREATE TABLE pi_2 PARTITION OF pi FOR VALUES FROM (10) TO (20) PARTITION BY RANGE (a)",
        )
        .unwrap();
        sql.execute("CREATE TABLE pi_2a PARTITION OF pi_2 FOR VALUES FROM (10) TO (15)")
            .unwrap();
        sql.execute("CREATE INDEX pi_b ON pi (b)").unwrap();
        // A partition created later and one attached later get the index;
        // an equivalent existing index is adopted instead of duplicated.
        sql.execute("CREATE TABLE pi_3 PARTITION OF pi FOR VALUES FROM (20) TO (30)")
            .unwrap();
        sql.execute("CREATE TABLE pi_4 (a int, b int)").unwrap();
        sql.execute("CREATE INDEX pi_4_mine ON pi_4 (b)").unwrap();
        sql.execute("ALTER TABLE pi ATTACH PARTITION pi_4 FOR VALUES FROM (30) TO (40)")
            .unwrap();
        sql.execute("INSERT INTO pi VALUES (1, 1), (12, 2), (25, 3), (35, 4)")
            .unwrap();
    }
    db.close().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        partition_indexes(&mut sql, "pi%"),
        vec![
            vec![s("pi"), s("pi_b"), s("I")],
            vec![s("pi_1"), s("pi_1_b_idx"), s("i")],
            vec![s("pi_2"), s("pi_2_b_idx"), s("I")],
            vec![s("pi_2a"), s("pi_2a_b_idx"), s("i")],
            vec![s("pi_3"), s("pi_3_b_idx"), s("i")],
            vec![s("pi_4"), s("pi_4_mine"), s("i")],
        ]
    );
    assert_eq!(
        rows(
            &mut sql,
            "SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'pi_b'::regclass ORDER BY 1"
        ),
        vec![
            vec![s("pi_1_b_idx")],
            vec![s("pi_2_b_idx")],
            vec![s("pi_3_b_idx")],
            vec![s("pi_4_mine")],
        ]
    );
    let error = sql.execute("DROP INDEX pi_1_b_idx").unwrap_err();
    assert_eq!(error.sqlstate(), "2BP01");
    assert_eq!(
        error.postgres_message(),
        "cannot drop index pi_1_b_idx because index pi_b requires it"
    );
    sql.execute("DROP INDEX pi_b").unwrap();
    assert_eq!(
        partition_indexes(&mut sql, "pi%"),
        Vec::<Vec<SqlValue>>::new()
    );
    assert_eq!(
        rows(&mut sql, "SELECT count(*) FROM pi WHERE b = 3"),
        vec![vec![SqlValue::Int(1)]]
    );
}

#[test]
fn unique_index_on_partitioned_table_needs_the_key_and_is_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE pu (a int, b int) PARTITION BY LIST (a)")
        .unwrap();
    sql.execute("CREATE TABLE pu_1 PARTITION OF pu FOR VALUES IN (1)")
        .unwrap();
    sql.execute("CREATE TABLE pu_2 PARTITION OF pu FOR VALUES IN (2)")
        .unwrap();
    let error = sql
        .execute("CREATE UNIQUE INDEX pu_b ON pu (b)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "0A000");
    assert_eq!(
        error.postgres_message(),
        "unique constraint on partitioned table must include all partitioning columns"
    );
    sql.execute("CREATE UNIQUE INDEX pu_ab ON pu (a, b)")
        .unwrap();
    sql.execute("INSERT INTO pu VALUES (1, 1), (2, 1)").unwrap();
    assert_eq!(
        sql.execute("INSERT INTO pu VALUES (1, 1)")
            .unwrap_err()
            .sqlstate(),
        "23505"
    );
}

#[test]
fn detached_partition_keeps_its_index_as_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE pd (a int, b int) PARTITION BY LIST (a)")
        .unwrap();
    sql.execute("CREATE TABLE pd_1 PARTITION OF pd FOR VALUES IN (1)")
        .unwrap();
    sql.execute("CREATE INDEX pd_b ON pd (b)").unwrap();
    sql.execute("ALTER TABLE pd DETACH PARTITION pd_1").unwrap();
    sql.execute("DROP INDEX pd_1_b_idx").unwrap();
}

#[test]
fn checks_reach_partitions_and_attach_requires_them() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE pc (a int, b int CONSTRAINT pc_b CHECK (b > 0)) PARTITION BY LIST (a)",
    )
    .unwrap();
    sql.execute("CREATE TABLE pc_1 PARTITION OF pc FOR VALUES IN (1)")
        .unwrap();
    sql.execute("ALTER TABLE pc ADD CONSTRAINT pc_b2 CHECK (b < 100)")
        .unwrap();
    sql.execute("ALTER TABLE pc ALTER COLUMN a SET NOT NULL")
        .unwrap();
    // Added to the parent, enforced on direct inserts into the partition.
    let error = sql.execute("INSERT INTO pc_1 VALUES (1, 200)").unwrap_err();
    assert_eq!(
        (error.sqlstate(), error.postgres_message().as_str()),
        (
            "23514",
            "new row for relation \"pc_1\" violates check constraint \"pc_b2\""
        )
    );
    sql.execute("CREATE TABLE pc_2 (a int NOT NULL, b int)")
        .unwrap();
    let error = sql
        .execute("ALTER TABLE pc ATTACH PARTITION pc_2 FOR VALUES IN (2)")
        .unwrap_err();
    assert_eq!(
        (error.sqlstate(), error.postgres_message().as_str()),
        ("42804", "child table is missing constraint \"pc_b\"")
    );
    sql.execute(
        "CREATE TABLE pc_3 (a int NOT NULL, b int CONSTRAINT pc_b CHECK (b > 0) CONSTRAINT pc_b2 CHECK (b < 100))",
    )
    .unwrap();
    sql.execute("ALTER TABLE pc ATTACH PARTITION pc_3 FOR VALUES IN (3)")
        .unwrap();
}

#[test]
fn unnamed_indexes_get_postgresql_names() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE nm (a int, b text) PARTITION BY LIST (a)")
        .unwrap();
    sql.execute("CREATE TABLE nm_1 PARTITION OF nm FOR VALUES IN (1)")
        .unwrap();
    sql.execute("CREATE INDEX ON nm (a, b)").unwrap();
    sql.execute("CREATE INDEX ON nm (a, b)").unwrap();
    sql.execute("CREATE INDEX ON nm (lower(b))").unwrap();
    assert_eq!(
        partition_indexes(&mut sql, "nm%")
            .into_iter()
            .map(|row| row[1].clone())
            .collect::<Vec<_>>(),
        vec![
            s("nm_a_b_idx"),
            s("nm_a_b_idx1"),
            s("nm_lower_idx"),
            s("nm_1_a_b_idx"),
            s("nm_1_a_b_idx1"),
            s("nm_1_lower_idx"),
        ]
    );
}
