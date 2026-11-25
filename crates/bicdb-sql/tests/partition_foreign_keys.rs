//! Foreign keys and partitioned tables, as PostgreSQL reports them: a key
//! referencing a partitioned table protects the rows of every partition
//! (also when the partition is named directly) and has a `<name>_<n>`
//! constraint row per referenced partition; a key declared on a partitioned
//! table is reported by that table and checked when a partition is attached.
use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn s(value: &str) -> SqlValue {
    SqlValue::String(value.into())
}

fn err(sql: &mut SqlSession<'_>, statement: &str) -> (String, String) {
    let error = sql.execute(statement).unwrap_err();
    (error.sqlstate().to_string(), error.postgres_message())
}

#[test]
fn keys_referencing_a_partitioned_table() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE TABLE rp (a int PRIMARY KEY) PARTITION BY RANGE (a)",
        "CREATE TABLE rp_1 PARTITION OF rp FOR VALUES FROM (0) TO (10)",
        "CREATE TABLE rp_2 PARTITION OF rp FOR VALUES FROM (10) TO (20)",
        "CREATE TABLE rc (x int REFERENCES rp)",
        "INSERT INTO rp VALUES (1), (15)",
        "INSERT INTO rc VALUES (1), (15), (NULL)",
    ] {
        sql.execute(statement).unwrap();
    }
    assert_eq!(
        err(&mut sql, "INSERT INTO rc VALUES (5)").0,
        "23503".to_string()
    );
    for statement in ["DELETE FROM rp WHERE a = 1", "DELETE FROM rp_1 WHERE a = 1"] {
        assert_eq!(
            err(&mut sql, statement),
            (
                "23503".into(),
                "update or delete on table \"rp_1\" violates foreign key constraint \"rc_x_fkey_1\" on table \"rc\"".into()
            ),
            "{statement}"
        );
    }
    assert_eq!(
        err(&mut sql, "DELETE FROM rp WHERE a = 15").1,
        "update or delete on table \"rp_2\" violates foreign key constraint \"rc_x_fkey_2\" on table \"rc\""
    );
    assert_eq!(
        sql.execute(
            "SELECT conname, confrelid::regclass::text FROM pg_constraint WHERE contype = 'f' AND conrelid = 'rc'::regclass ORDER BY 1"
        )
        .unwrap()
        .rows,
        vec![
            vec![s("rc_x_fkey"), s("rp")],
            vec![s("rc_x_fkey_1"), s("rp_1")],
            vec![s("rc_x_fkey_2"), s("rp_2")],
        ]
    );
}

#[test]
fn keys_declared_on_a_partitioned_table() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE TABLE dp (a int PRIMARY KEY)",
        "CREATE TABLE dc (a int REFERENCES dp, b int) PARTITION BY LIST (b)",
        "CREATE TABLE dc_1 PARTITION OF dc FOR VALUES IN (1)",
        "INSERT INTO dp VALUES (1)",
        "INSERT INTO dc VALUES (1, 1)",
    ] {
        sql.execute(statement).unwrap();
    }
    assert_eq!(
        err(&mut sql, "DELETE FROM dp").1,
        "update or delete on table \"dp\" violates foreign key constraint \"dc_a_fkey\" on table \"dc\""
    );
    assert_eq!(
        sql.execute(
            "SELECT conrelid::regclass::text, conparentid <> 0 FROM pg_constraint WHERE contype = 'f' AND conname = 'dc_a_fkey' ORDER BY 1"
        )
        .unwrap()
        .rows,
        vec![
            vec![s("dc"), SqlValue::Bool(false)],
            vec![s("dc_1"), SqlValue::Bool(true)],
        ]
    );
    sql.execute("CREATE TABLE dc_2 (a int, b int)").unwrap();
    sql.execute("INSERT INTO dc_2 VALUES (9, 2)").unwrap();
    assert_eq!(
        err(
            &mut sql,
            "ALTER TABLE dc ATTACH PARTITION dc_2 FOR VALUES IN (2)"
        )
        .0,
        "23503"
    );
}
