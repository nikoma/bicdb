//! PostgreSQL parity: soft input errors (`pg_input_is_valid`,
//! `pg_input_error_info`), VACUUM/ANALYZE statement forms, and FROM-less
//! SELECT evaluation. Expected values are PostgreSQL 18's.

use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};
use tempfile::TempDir;

fn open() -> (TempDir, BicDb) {
    let dir = TempDir::new().unwrap();
    let db = BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    (dir, db)
}

fn text(value: &SqlValue) -> Option<String> {
    match value {
        SqlValue::Null => None,
        other => Some(other.to_cell()),
    }
}

fn error_info(sql: &mut SqlSession<'_>, input: &str, type_name: &str) -> Vec<Option<String>> {
    let result = sql
        .execute(&format!(
            "SELECT * FROM pg_input_error_info('{input}', '{type_name}')"
        ))
        .unwrap();
    assert_eq!(
        result.columns,
        ["message", "detail", "hint", "sql_error_code"],
        "{result:?}"
    );
    assert_eq!(result.rows.len(), 1);
    result.rows[0].iter().map(text).collect()
}

#[test]
fn pg_input_error_info_reports_soft_input_errors() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        error_info(&mut sql, "junk", "int4"),
        [
            Some("invalid input syntax for type integer: \"junk\"".to_string()),
            None,
            None,
            Some("22P02".to_string()),
        ]
    );
    assert_eq!(
        error_info(&mut sql, "50000", "int2"),
        [
            Some("value \"50000\" is out of range for type smallint".to_string()),
            None,
            None,
            Some("22003".to_string()),
        ]
    );
    assert_eq!(error_info(&mut sql, "42", "int4"), [None, None, None, None]);
    let result = sql
        .execute("SELECT message, sql_error_code FROM pg_input_error_info('junk', 'bool') AS e")
        .unwrap();
    assert_eq!(
        result.rows[0].iter().map(text).collect::<Vec<_>>(),
        [
            Some("invalid input syntax for type boolean: \"junk\"".to_string()),
            Some("22P02".to_string()),
        ]
    );
}

#[test]
fn pg_input_soft_errors_cover_user_types_and_raise_hard_errors() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TYPE soft_rainbow AS ENUM ('red', 'green')")
        .unwrap();
    sql.execute("CREATE DOMAIN soft_positive AS int CHECK (value > 0)")
        .unwrap();
    let valid = sql
        .execute(
            "SELECT pg_input_is_valid('red', 'soft_rainbow'), \
             pg_input_is_valid('mauve', 'soft_rainbow'), \
             pg_input_is_valid('1', 'soft_positive'), \
             pg_input_is_valid('-1', 'soft_positive'), \
             pg_input_is_valid('junk', 'soft_positive'), \
             pg_input_is_valid(NULL, 'int4')",
        )
        .unwrap();
    assert_eq!(
        valid.rows[0],
        [
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(true),
            SqlValue::Bool(false),
            SqlValue::Bool(false),
            SqlValue::Null,
        ]
    );
    assert_eq!(
        error_info(&mut sql, "-1", "soft_positive"),
        [
            Some(
                "value for domain soft_positive violates check constraint \"soft_positive_check\""
                    .to_string()
            ),
            None,
            None,
            Some("23514".to_string()),
        ]
    );
    // An undefined type is not an input error: it is raised.
    let error = sql
        .execute("SELECT pg_input_is_valid('1', 'no_such_type')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42704");
    assert_eq!(error.to_string(), "type \"no_such_type\" does not exist");
    let error = sql
        .execute("SELECT * FROM pg_input_error_info('1', 'no_such_type')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "42704");
    // An error raised while evaluating a domain CHECK expression is not
    // trapped either.
    sql.execute("CREATE DOMAIN soft_weird AS float8 CHECK (1 / value > 0)")
        .unwrap();
    let error = sql
        .execute("SELECT * FROM pg_input_error_info('0', 'soft_weird')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22012", "{error}");
}

#[test]
fn vacuum_and_analyze_accept_postgres_forms() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE vac_t (a int, b text)").unwrap();
    sql.execute("INSERT INTO vac_t VALUES (1, 'x'), (2, 'y')")
        .unwrap();
    for (statement, tag) in [
        ("VACUUM vac_t", "VACUUM"),
        ("VACUUM (ANALYZE) vac_t", "VACUUM"),
        ("VACUUM ANALYZE vac_t", "VACUUM"),
        ("VACUUM FULL vac_t", "VACUUM"),
        ("VACUUM FREEZE VERBOSE vac_t", "VACUUM"),
        ("VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) vac_t", "VACUUM"),
        ("VACUUM (PARALLEL 2, INDEX_CLEANUP off) vac_t", "VACUUM"),
        ("VACUUM ANALYZE vac_t (a, b)", "VACUUM"),
        ("VACUUM ANALYZE", "VACUUM"),
        ("VACUUM (ONLY_DATABASE_STATS)", "VACUUM"),
        ("ANALYZE vac_t", "ANALYZE"),
        ("ANALYZE VERBOSE vac_t (a)", "ANALYZE"),
        ("ANALYZE (VERBOSE, SKIP_LOCKED) vac_t", "ANALYZE"),
        ("ANALYZE public.vac_t", "ANALYZE"),
    ] {
        let result = sql.execute(statement).unwrap_or_else(|error| {
            panic!("{statement}: {error}");
        });
        assert_eq!(result.command_tag.as_deref(), Some(tag), "{statement}");
        assert!(result.rows.is_empty(), "{statement}");
    }
    // VACUUM ANALYZE refreshed the statistics.
    let stats = sql
        .execute("SELECT count(*) FROM pg_stats WHERE tablename = 'vac_t'")
        .unwrap();
    assert_ne!(stats.rows[0][0], SqlValue::Int(0), "{stats:?}");
}

#[test]
fn vacuum_and_analyze_errors_match_postgres() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE vac_e (a int, b int)").unwrap();
    for (statement, sqlstate, message) in [
        (
            "VACUUM vac_e, vac_missing",
            "42P01",
            "relation \"vac_missing\" does not exist",
        ),
        (
            "ANALYZE vac_missing",
            "42P01",
            "relation \"vac_missing\" does not exist",
        ),
        (
            "VACUUM (nonexistentarg) vac_e",
            "42601",
            "unrecognized VACUUM option \"nonexistentarg\"",
        ),
        (
            "ANALYZE (nonexistentarg) vac_e",
            "42601",
            "unrecognized ANALYZE option \"nonexistentarg\"",
        ),
        (
            "VACUUM vac_e (a)",
            "0A000",
            "ANALYZE option must be specified when a column list is provided",
        ),
        (
            "ANALYZE vac_e (nope)",
            "42703",
            "column \"nope\" of relation \"vac_e\" does not exist",
        ),
        (
            "ANALYZE vac_e (a, b, b)",
            "42701",
            "column \"b\" of relation \"vac_e\" appears more than once",
        ),
        (
            "VACUUM (PARALLEL 2, FULL TRUE) vac_e",
            "0A000",
            "VACUUM FULL cannot be performed in parallel",
        ),
        (
            "VACUUM (PARALLEL) vac_e",
            "42601",
            "parallel option requires a value between 0 and 1024",
        ),
        (
            "VACUUM (PARALLEL -1) vac_e",
            "42601",
            "parallel workers for vacuum must be between 0 and 1024",
        ),
        (
            "VACUUM (ONLY_DATABASE_STATS) vac_e",
            "0A000",
            "ONLY_DATABASE_STATS cannot be specified with a list of tables",
        ),
    ] {
        let error = sql.execute(statement).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{statement}: {error}");
        assert_eq!(error.to_string(), message, "{statement}");
    }
    sql.execute("BEGIN").unwrap();
    for statement in ["VACUUM vac_e", "VACUUM"] {
        let error = sql.execute(statement).unwrap_err();
        assert_eq!(error.sqlstate(), "25001", "{statement}");
        assert_eq!(
            error.to_string(),
            "VACUUM cannot run inside a transaction block"
        );
        sql.execute("ROLLBACK").unwrap();
        sql.execute("BEGIN").unwrap();
    }
    // ANALYZE may run inside a transaction block.
    sql.execute("ANALYZE vac_e").unwrap();
    sql.execute("COMMIT").unwrap();
}

#[test]
fn vacuum_skips_views_and_foreign_tables_with_postgres_warnings() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE vac_w (a int)").unwrap();
    sql.execute("CREATE VIEW vac_wv AS SELECT a FROM vac_w")
        .unwrap();
    let result = sql.execute("VACUUM vac_wv").unwrap();
    assert_eq!(
        result
            .notices
            .iter()
            .map(|notice| notice.message.as_str())
            .collect::<Vec<_>>(),
        ["skipping \"vac_wv\" --- cannot vacuum non-tables or special system tables"]
    );
    assert!(result.notices[0]
        .fields
        .contains(&bicdb_sql::SqlErrorField::Severity("WARNING".into())));
    sql.execute("CREATE ROLE vac_other").unwrap();
    sql.execute("SET ROLE vac_other").unwrap();
    for (statement, message) in [
        (
            "VACUUM vac_w",
            "permission denied to vacuum \"vac_w\", skipping it",
        ),
        (
            "VACUUM (ANALYZE) vac_w",
            "permission denied to vacuum \"vac_w\", skipping it",
        ),
        (
            "ANALYZE vac_w",
            "permission denied to analyze \"vac_w\", skipping it",
        ),
    ] {
        let result = sql.execute(statement).unwrap();
        assert_eq!(
            result
                .notices
                .iter()
                .map(|notice| notice.message.as_str())
                .collect::<Vec<_>>(),
            [message],
            "{statement}"
        );
    }
}

#[derive(Debug)]
struct TestBackend(i32);

impl bicdb_sql::SqlSessionRuntime for TestBackend {
    fn backend_pid(&self) -> i32 {
        self.0
    }

    fn execute_advisory_lock(
        &self,
        _name: &str,
        _args: &[SqlValue],
        _cancellation: &bicdb_core::CancellationToken,
    ) -> bicdb_sql::Result<Option<SqlValue>> {
        Ok(None)
    }

    fn advisory_lock_rows(
        &self,
        _database_oid: i64,
    ) -> Vec<std::collections::BTreeMap<String, SqlValue>> {
        Vec::new()
    }
}

fn rows(sql: &mut SqlSession<'_>, statement: &str) -> Vec<Vec<SqlValue>> {
    sql.execute(statement)
        .unwrap_or_else(|error| panic!("{statement}: {error}"))
        .rows
}

#[test]
fn temporary_tables_shadow_permanent_ones_and_end_with_the_session() {
    let (_dir, mut db) = open();
    {
        let mut sql = SqlSession::new(&mut db).with_runtime(std::sync::Arc::new(TestBackend(7)));
        sql.execute("CREATE TABLE tt_shadow (col int)").unwrap();
        sql.execute("INSERT INTO tt_shadow VALUES (1)").unwrap();
        sql.execute("CREATE INDEX i_tt_shadow ON tt_shadow (col)")
            .unwrap();
        sql.execute("CREATE TEMP TABLE tt_shadow (tcol float8)")
            .unwrap();
        sql.execute("CREATE INDEX i_tt_shadow ON tt_shadow (tcol)")
            .unwrap();
        sql.execute("INSERT INTO tt_shadow VALUES (2.5)").unwrap();
        assert_eq!(
            rows(&mut sql, "SELECT * FROM tt_shadow"),
            [[SqlValue::Float(2.5)]]
        );
        assert_eq!(
            rows(&mut sql, "SELECT * FROM public.tt_shadow"),
            [[SqlValue::Int(1)]]
        );
        assert_eq!(
            rows(&mut sql, "SELECT * FROM pg_temp.tt_shadow"),
            [[SqlValue::Float(2.5)]]
        );
        assert_eq!(
            rows(&mut sql, "SELECT current_schema()"),
            [[SqlValue::String("public".into())]]
        );
        assert_eq!(
            rows(
                &mut sql,
                "SELECT c.relpersistence::text, n.nspname::text FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relname = 'tt_shadow' ORDER BY 1"
            ),
            [
                [
                    SqlValue::String("p".into()),
                    SqlValue::String("public".into())
                ],
                [
                    SqlValue::String("t".into()),
                    SqlValue::String("pg_temp_7".into())
                ],
            ]
        );
        // DROP finds the temporary relations first, then the permanent ones.
        sql.execute("DROP INDEX i_tt_shadow").unwrap();
        sql.execute("DROP TABLE tt_shadow").unwrap();
        assert_eq!(
            rows(&mut sql, "SELECT * FROM tt_shadow"),
            [[SqlValue::Int(1)]]
        );
        sql.execute("CREATE TEMP TABLE tt_session (a int)").unwrap();
        sql.execute("INSERT INTO tt_session VALUES (3)").unwrap();
        // A new permanent table never lands in the temporary schema.
        sql.execute("CREATE TABLE tt_permanent (a int)").unwrap();
        assert_eq!(
            rows(
                &mut sql,
                "SELECT n.nspname::text FROM pg_class c JOIN pg_namespace n \
                 ON n.oid = c.relnamespace WHERE c.relname = 'tt_permanent'"
            ),
            [[SqlValue::String("public".into())]]
        );
    }
    assert!(bicdb_sql::temp_schema_exists(&db, 7));
    assert!(bicdb_sql::drop_connection_temp_schema(&mut db, 7).unwrap());
    assert!(!bicdb_sql::temp_schema_exists(&db, 7));
    let mut sql = SqlSession::new(&mut db).with_runtime(std::sync::Arc::new(TestBackend(8)));
    let error = sql.execute("SELECT * FROM tt_session").unwrap_err();
    assert_eq!(error.sqlstate(), "42P01", "{error}");
    sql.execute("CREATE TEMP TABLE tt_session (a int)").unwrap();
    sql.execute("DISCARD TEMP").unwrap();
    assert_eq!(
        sql.execute("SELECT * FROM tt_session")
            .unwrap_err()
            .sqlstate(),
        "42P01"
    );
}

#[test]
fn temporary_table_placement_errors_match_postgres() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db).with_runtime(std::sync::Arc::new(TestBackend(9)));
    sql.execute("CREATE SCHEMA tt_s").unwrap();
    for (statement, sqlstate, message) in [
        (
            "CREATE TEMP TABLE tt_s.t (a int)",
            "42P16",
            "cannot create temporary relation in non-temporary schema",
        ),
        (
            "CREATE TABLE tt_oc (a int) ON COMMIT DELETE ROWS",
            "42P16",
            "ON COMMIT can only be used on temporary tables",
        ),
    ] {
        let error = sql.execute(statement).unwrap_err();
        assert_eq!(error.sqlstate(), sqlstate, "{statement}");
        assert_eq!(error.to_string(), message, "{statement}");
    }
    sql.execute("CREATE TEMP TABLE tt_ok (a int) ON COMMIT PRESERVE ROWS")
        .unwrap();
    sql.execute("CREATE TABLE pg_temp.tt_explicit (a int)")
        .unwrap();
    assert_eq!(
        rows(
            &mut sql,
            "SELECT relpersistence::text FROM pg_class WHERE relname = 'tt_explicit'"
        ),
        [[SqlValue::String("t".into())]]
    );
}

#[test]
fn orphan_temporary_schemas_are_dropped() {
    let (_dir, mut db) = open();
    {
        let mut sql = SqlSession::new(&mut db).with_runtime(std::sync::Arc::new(TestBackend(11)));
        sql.execute("CREATE TEMP TABLE tt_orphan (a int)").unwrap();
    }
    assert_eq!(bicdb_sql::drop_orphan_temp_schemas(&mut db).unwrap(), 1);
    assert!(!bicdb_sql::temp_schema_exists(&db, 11));
}

#[test]
fn temporary_table_on_commit_actions() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db).with_runtime(std::sync::Arc::new(TestBackend(21)));
    sql.execute("CREATE TEMP TABLE tt_delete (a int) ON COMMIT DELETE ROWS")
        .unwrap();
    sql.execute("BEGIN").unwrap();
    sql.execute("INSERT INTO tt_delete VALUES (1), (2)")
        .unwrap();
    assert_eq!(
        rows(&mut sql, "SELECT count(*) FROM tt_delete"),
        [[SqlValue::Int(2)]]
    );
    sql.execute("COMMIT").unwrap();
    assert_eq!(
        rows(&mut sql, "SELECT count(*) FROM tt_delete"),
        [[SqlValue::Int(0)]]
    );
    // Outside a transaction block every statement commits.
    sql.execute("INSERT INTO tt_delete VALUES (3)").unwrap();
    assert_eq!(
        rows(&mut sql, "SELECT count(*) FROM tt_delete"),
        [[SqlValue::Int(0)]]
    );
    sql.execute("BEGIN").unwrap();
    sql.execute("CREATE TEMP TABLE tt_drop (a int) ON COMMIT DROP")
        .unwrap();
    sql.execute("INSERT INTO tt_drop VALUES (1)").unwrap();
    assert_eq!(
        rows(&mut sql, "SELECT count(*) FROM tt_drop"),
        [[SqlValue::Int(1)]]
    );
    sql.execute("COMMIT").unwrap();
    assert_eq!(
        sql.execute("SELECT * FROM tt_drop").unwrap_err().sqlstate(),
        "42P01"
    );
}

#[test]
fn embedded_session_temporary_tables_end_with_the_session() {
    let (_dir, mut db) = open();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE tt_emb (a int)").unwrap();
        sql.execute("CREATE TEMP TABLE tt_emb (b int)").unwrap();
        sql.execute("INSERT INTO tt_emb VALUES (5)").unwrap();
        assert_eq!(rows(&mut sql, "SELECT b FROM tt_emb"), [[SqlValue::Int(5)]]);
        assert_eq!(
            rows(&mut sql, "SELECT count(*) FROM public.tt_emb"),
            [[SqlValue::Int(0)]]
        );
    }
    let mut sql = SqlSession::new(&mut db);
    // The permanent table remains; the temporary one is gone.
    assert_eq!(
        sql.execute("SELECT * FROM tt_emb").unwrap().columns,
        ["a".to_string()]
    );
    assert_eq!(
        rows(
            &mut sql,
            "SELECT count(*) FROM pg_class WHERE relname = 'tt_emb' AND relpersistence = 't'"
        ),
        [[SqlValue::Int(0)]]
    );
}

#[test]
fn integer_text_input_is_not_rounded() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    for (statement, message) in [
        (
            "SELECT '34.5'::int2",
            "invalid input syntax for type smallint: \"34.5\"",
        ),
        (
            "SELECT int4 '1e3'",
            "invalid input syntax for type integer: \"1e3\"",
        ),
        (
            "SELECT '1.5'::text::int8",
            "invalid input syntax for type bigint: \"1.5\"",
        ),
    ] {
        let error = sql.execute(statement).unwrap_err();
        assert_eq!(error.sqlstate(), "22P02", "{statement}");
        assert_eq!(error.to_string(), message, "{statement}");
    }
    sql.execute("CREATE TABLE it_t (a int2, b int8)").unwrap();
    let error = sql
        .execute("INSERT INTO it_t VALUES ('34.5', 1)")
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid input syntax for type smallint: \"34.5\""
    );
    sql.execute("INSERT INTO it_t VALUES (' 12 ', '0x10'), (34.5, 2.5)")
        .unwrap();
    let error = sql.execute("UPDATE it_t SET a = '2.5'").unwrap_err();
    assert_eq!(error.sqlstate(), "22P02");
    assert_eq!(
        rows(&mut sql, "SELECT a, b FROM it_t ORDER BY a"),
        [
            [SqlValue::Int(12), SqlValue::Int(16)],
            [SqlValue::Int(35), SqlValue::Int(3)],
        ]
    );
    // Numeric values keep PostgreSQL's rounding.
    assert_eq!(
        rows(&mut sql, "SELECT 34.5::numeric::int2, (-2.5)::int8"),
        [[SqlValue::Int(35), SqlValue::Int(-3)]]
    );
}

fn cell(sql: &mut SqlSession<'_>, statement: &str) -> String {
    rows(sql, statement)[0][0].to_cell()
}

#[test]
fn describe_catalog_text_matches_postgres() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    sql.execute(
        "CREATE TABLE dc_t (a int, c numeric, d float8, e text, CHECK (c > 0), \
         CHECK (d < 10 AND a >= 0), CHECK (e <> ''))",
    )
    .unwrap();
    assert_eq!(
        rows(
            &mut sql,
            "SELECT conname::text, pg_get_constraintdef(oid, true) FROM pg_constraint \
             WHERE conrelid = 'dc_t'::regclass AND contype = 'c' ORDER BY 1"
        ),
        [
            [
                SqlValue::String("dc_t_c_check".into()),
                SqlValue::String("CHECK (c > 0::numeric)".into())
            ],
            [
                SqlValue::String("dc_t_check".into()),
                SqlValue::String("CHECK (d < 10::double precision AND a >= 0)".into())
            ],
            [
                SqlValue::String("dc_t_e_check".into()),
                SqlValue::String("CHECK (e <> ''::text)".into())
            ],
        ]
    );
    sql.execute("CREATE VIEW dc_v AS SELECT a, e FROM dc_t WHERE a > 1 ORDER BY e")
        .unwrap();
    assert_eq!(
        cell(&mut sql, "SELECT pg_get_viewdef('dc_v'::regclass, true)"),
        " SELECT a,\n    e\n   FROM dc_t\n  WHERE a > 1\n  ORDER BY e;"
    );
    assert_eq!(
        cell(
            &mut sql,
            "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'public'"
        ),
        "pg_database_owner"
    );
    assert_eq!(
        cell(
            &mut sql,
            "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = 'bicdb_graph_nodes'"
        ),
        "pg_catalog"
    );
    sql.execute("CREATE INDEX dc_i ON dc_t (e, a)").unwrap();
    assert_eq!(
        rows(
            &mut sql,
            "SELECT a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod) \
             FROM pg_attribute a WHERE a.attrelid = 'dc_i'::regclass AND a.attnum > 0 \
             ORDER BY a.attnum"
        ),
        [
            [
                SqlValue::String("e".into()),
                SqlValue::String("text".into())
            ],
            [
                SqlValue::String("a".into()),
                SqlValue::String("integer".into())
            ],
        ]
    );
    sql.execute("CREATE TABLE dc_p (k int) PARTITION BY RANGE (k)")
        .unwrap();
    sql.execute("CREATE TABLE dc_c PARTITION OF dc_p FOR VALUES FROM (0) TO (10)")
        .unwrap();
    assert_eq!(
        rows(
            &mut sql,
            "SELECT count(*) FROM (SELECT pg_partition_ancestors('dc_c'::regclass)) s"
        ),
        [[SqlValue::Int(2)]]
    );
}

#[test]
fn added_scalar_functions_and_input_details() {
    let (_dir, mut db) = open();
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(cell(&mut sql, "SELECT div(9, 4)"), "2");
    assert_eq!(
        cell(&mut sql, "SELECT factorial(20)"),
        "2432902008176640000"
    );
    assert_eq!(
        rows(
            &mut sql,
            "SELECT width_bucket(5.35, 0.024, 10.06, 5), width_bucket(11::float8, 0, 10, 5)"
        ),
        [[SqlValue::Int(3), SqlValue::Int(6)]]
    );
    assert_eq!(
        rows(&mut sql, "SELECT sind(30), cosd(60), tand(45)"),
        [[
            SqlValue::Float(0.5),
            SqlValue::Float(0.5),
            SqlValue::Float(1.0)
        ]]
    );
    assert_eq!(
        cell(&mut sql, "SELECT array_reverse(ARRAY[1,2,3])::text"),
        "{3,2,1}"
    );
    assert_eq!(
        cell(&mut sql, "SELECT array_sort(ARRAY[3,1,2])::text"),
        "{1,2,3}"
    );
    for (input, type_name, message, detail, code) in [
        (
            "(1,4",
            "int4range",
            "malformed range literal: \"(1,4\"",
            Some("Unexpected end of input."),
            "22P02",
        ),
        (
            "(4,zed)",
            "int4range",
            "invalid input syntax for type integer: \"zed\"",
            None,
            "22P02",
        ),
        (
            "192.168.198.200/24",
            "cidr",
            "invalid cidr value: \"192.168.198.200/24\"",
            Some("Value has bits set to right of mask."),
            "22P02",
        ),
        (
            "0xffffffffff",
            "xid",
            "value \"0xffffffffff\" is out of range for type xid",
            None,
            "22003",
        ),
    ] {
        assert_eq!(
            error_info(&mut sql, input, type_name),
            [
                Some(message.to_string()),
                detail.map(str::to_string),
                None,
                Some(code.to_string())
            ],
            "{input}::{type_name}"
        );
    }
    let error = sql.execute("SELECT '\\xDeAdBeE'::bytea").unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid hexadecimal data: odd number of digits"
    );
}
