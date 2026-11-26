//! Expression partition keys (`PARTITION BY RANGE ((b + 0))`, `LIST (lower(a))`):
//! routing, direct-partition bound checks, catalog output and the CREATE-time
//! rules, as PostgreSQL applies them. The definitions must survive a reopen.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn s(value: &str) -> SqlValue {
    SqlValue::String(value.into())
}

fn rows(sql: &mut SqlSession<'_>, query: &str) -> Vec<Vec<SqlValue>> {
    sql.execute(query).unwrap().rows
}

fn err(sql: &mut SqlSession<'_>, statement: &str) -> (String, String) {
    let error = sql.execute(statement).unwrap_err();
    (error.sqlstate().to_string(), error.postgres_message())
}

#[test]
fn range_key_with_expression_routes_and_checks_direct_inserts() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default().with_storage_mode(mode);
        {
            let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE er (a text, b int) PARTITION BY RANGE (a, (b+0))")
                .unwrap();
            assert_eq!(
                err(&mut sql, "INSERT INTO er VALUES ('a', 11)"),
                (
                    "23514".into(),
                    "no partition of relation \"er\" found for row".into()
                )
            );
            sql.execute("CREATE TABLE er1 PARTITION OF er FOR VALUES FROM ('a', 1) TO ('a', 10)")
                .unwrap();
            sql.execute("CREATE TABLE er2 PARTITION OF er FOR VALUES FROM ('a', 10) TO ('a', 20)")
                .unwrap();
            sql.execute("CREATE TABLE erd PARTITION OF er DEFAULT")
                .unwrap();
            sql.execute("INSERT INTO er VALUES ('a', 1), ('a', 10), ('a', 20), ('a', NULL)")
                .unwrap();
            for statement in [
                "INSERT INTO er1 VALUES ('a', 11)",
                "INSERT INTO er1 VALUES ('a')",
                "INSERT INTO erd VALUES ('a', 5)",
            ] {
                assert_eq!(err(&mut sql, statement).0, "23514", "{statement}");
            }
            sql.execute("INSERT INTO er1 VALUES ('a', 9)").unwrap();
            db.close().unwrap();
        }
        let mut db = BicDb::open_with_config(dir.path(), cfg).unwrap();
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(
            rows(
                &mut sql,
                "SELECT tableoid::regclass::text, a, b FROM er ORDER BY 1, 3"
            ),
            vec![
                vec![s("er1"), s("a"), SqlValue::Int(1)],
                vec![s("er1"), s("a"), SqlValue::Int(9)],
                vec![s("er2"), s("a"), SqlValue::Int(10)],
                vec![s("erd"), s("a"), SqlValue::Int(20)],
                vec![s("erd"), s("a"), SqlValue::Null],
            ]
        );
        assert_eq!(
            rows(&mut sql, "SELECT pg_get_partkeydef('er'::regclass)"),
            vec![vec![s("RANGE (a, ((b + 0)))")]]
        );
        assert_eq!(
            rows(
                &mut sql,
                "SELECT partattrs::text, partexprs IS NOT NULL FROM pg_partitioned_table WHERE partrelid = 'er'::regclass"
            ),
            vec![vec![s("1 0"), SqlValue::Bool(true)]]
        );
        // Routing after reopen still evaluates the expression.
        sql.execute("INSERT INTO er VALUES ('a', 15)").unwrap();
        assert_eq!(
            rows(
                &mut sql,
                "SELECT tableoid::regclass::text FROM er WHERE b = 15"
            ),
            vec![vec![s("er2")]]
        );
    }
}

#[test]
fn list_key_on_function_routes_case_insensitively_through_levels() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE el (a text, b int) PARTITION BY LIST (lower(a))")
        .unwrap();
    sql.execute("CREATE TABLE el_ab PARTITION OF el FOR VALUES IN ('aa', 'bb')")
        .unwrap();
    sql.execute("CREATE TABLE el_n PARTITION OF el FOR VALUES IN (NULL)")
        .unwrap();
    sql.execute(
        "CREATE TABLE el_ef PARTITION OF el FOR VALUES IN ('ee', 'ff') PARTITION BY RANGE (b)",
    )
    .unwrap();
    sql.execute("CREATE TABLE el_ef1 PARTITION OF el_ef FOR VALUES FROM (1) TO (10)")
        .unwrap();
    sql.execute("INSERT INTO el VALUES ('AA', 1), ('bB', 2), (NULL, 3), ('Ee', 4)")
        .unwrap();
    assert_eq!(
        err(&mut sql, "INSERT INTO el_ab VALUES ('AAa', 1)").0,
        "23514"
    );
    // The parent's expression constraint applies to a sub-partition too.
    assert_eq!(
        err(&mut sql, "INSERT INTO el_ef1 VALUES ('cc', 1)").0,
        "23514"
    );
    assert_eq!(
        rows(
            &mut sql,
            "SELECT tableoid::regclass::text, a FROM el ORDER BY 1, 2"
        ),
        vec![
            vec![s("el_ab"), s("AA")],
            vec![s("el_ab"), s("bB")],
            vec![s("el_ef1"), s("Ee")],
            vec![s("el_n"), SqlValue::Null],
        ]
    );
    assert_eq!(
        rows(&mut sql, "SELECT pg_get_partkeydef('el'::regclass)"),
        vec![vec![s("LIST (lower(a))")]]
    );
}

#[test]
fn multi_column_key_mixes_columns_and_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE em (a int, b int, c int) PARTITION BY RANGE (a, abs(b), c)")
        .unwrap();
    sql.execute(
        "CREATE TABLE em1 PARTITION OF em FOR VALUES FROM (1, minvalue, minvalue) TO (1, 10, 5)",
    )
    .unwrap();
    sql.execute(
        "CREATE TABLE em2 PARTITION OF em FOR VALUES FROM (1, 10, 5) TO (maxvalue, maxvalue, maxvalue)",
    )
    .unwrap();
    sql.execute("INSERT INTO em VALUES (1, -3, 1), (1, -10, 5), (1, 9, 100)")
        .unwrap();
    assert_eq!(
        rows(
            &mut sql,
            "SELECT tableoid::regclass::text, b FROM em ORDER BY 2"
        ),
        vec![
            vec![s("em2"), SqlValue::Int(-10)],
            vec![s("em1"), SqlValue::Int(-3)],
            vec![s("em1"), SqlValue::Int(9)],
        ]
    );
    // Overlap is judged on the expression's values as well.
    assert_eq!(
        err(
            &mut sql,
            "CREATE TABLE em3 PARTITION OF em FOR VALUES FROM (1, 5, 0) TO (1, 6, 0)"
        )
        .0,
        "42P17"
    );
}

#[test]
fn create_time_rules_for_expression_keys_match_postgresql() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), DbConfig::default()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for statement in [
        "CREATE FUNCTION ex_const() RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$",
        "CREATE FUNCTION ex_volatile(a int) RETURNS int LANGUAGE sql AS $$ SELECT a $$",
        "CREATE FUNCTION ex_set(a int) RETURNS SETOF int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$",
        "CREATE FUNCTION ex_immutable(a int) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT a $$",
    ] {
        sql.execute(statement).unwrap();
    }
    for (statement, sqlstate, message) in [
        (
            "CREATE TABLE ex1 (a int) PARTITION BY RANGE ((b + 1))",
            "42703",
            "column \"b\" does not exist",
        ),
        (
            "CREATE TABLE ex2 (a int) PARTITION BY RANGE (b)",
            "42703",
            "column \"b\" named in partition key does not exist",
        ),
        (
            "CREATE TABLE ex3 (a int) PARTITION BY RANGE ((1 + 1))",
            "42P17",
            "cannot use constant expression as partition key",
        ),
        (
            "CREATE TABLE ex4 (a int) PARTITION BY RANGE ((count(a)))",
            "42803",
            "aggregate functions are not allowed in partition key expressions",
        ),
        (
            "CREATE TABLE ex5 (a float8) PARTITION BY RANGE ((random() + a))",
            "42P17",
            "functions in partition key expression must be marked IMMUTABLE",
        ),
        (
            "CREATE TABLE ex6 (a int PRIMARY KEY) PARTITION BY RANGE ((a + 0))",
            "0A000",
            "unsupported PRIMARY KEY constraint with partition key definition",
        ),
        (
            "CREATE TABLE ex7 (a int, b int, PRIMARY KEY (a)) PARTITION BY RANGE (b)",
            "0A000",
            "unique constraint on partitioned table must include all partitioning columns",
        ),
        (
            "CREATE TABLE ex8 (a int, b int) PARTITION BY LIST (a, b)",
            "42P17",
            "cannot use \"list\" partition strategy with more than one column",
        ),
        (
            "CREATE TABLE ex9 (a int) PARTITION BY MAGIC (a)",
            "22023",
            "unrecognized partitioning strategy \"magic\"",
        ),
        (
            "CREATE TABLE ex10 (a int, b int) PARTITION BY RANGE (((a, b)))",
            "42P16",
            "partition key column 1 has pseudo-type record",
        ),
        (
            "CREATE TABLE ex11 (a int, b int) PARTITION BY RANGE (a, ('unknown'))",
            "42P16",
            "partition key column 2 has pseudo-type unknown",
        ),
        (
            "CREATE TABLE ex12 (a int) PARTITION BY RANGE (ex_const())",
            "42P17",
            "cannot use constant expression as partition key",
        ),
        (
            "CREATE TABLE ex13 (a int) PARTITION BY RANGE (ex_volatile(a))",
            "42P17",
            "functions in partition key expression must be marked IMMUTABLE",
        ),
        (
            "CREATE TABLE ex14 (a int) PARTITION BY RANGE (ex_set(a))",
            "0A000",
            "set-returning functions are not allowed in partition key expressions",
        ),
        (
            "CREATE TABLE ex15 (a int) PARTITION BY RANGE (ex_immutable(a))",
            "0A000",
            "user-defined functions in partition key expressions are not supported",
        ),
    ] {
        assert_eq!(
            err(&mut sql, statement),
            (sqlstate.to_string(), message.to_string()),
            "{statement}"
        );
    }
    sql.execute("CREATE TABLE ek (a int, b int) PARTITION BY RANGE ((b + 0))")
        .unwrap();
    assert_eq!(
        err(&mut sql, "ALTER TABLE ek DROP COLUMN b"),
        (
            "42P16".into(),
            "cannot drop column \"b\" because it is part of the partition key of relation \"ek\""
                .into()
        )
    );
    assert_eq!(
        err(&mut sql, "ALTER TABLE ek ALTER COLUMN b TYPE bigint").0,
        "42P16"
    );
    // A cast key is an expression: it orders by the cast type, not the column.
    sql.execute("CREATE TABLE ec (a int) PARTITION BY RANGE ((a::text))")
        .unwrap();
    sql.execute("CREATE TABLE ec1 PARTITION OF ec FOR VALUES FROM ('1') TO ('2')")
        .unwrap();
    sql.execute("INSERT INTO ec VALUES (10), (1)").unwrap();
    assert_eq!(err(&mut sql, "INSERT INTO ec VALUES (9)").0, "23514");
}
