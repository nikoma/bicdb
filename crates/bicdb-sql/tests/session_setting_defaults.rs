use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn set_default_restores_session_settings_with_local_scope() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    for command in [
        "SET work_mem = '64MB'",
        "SET jit_above_cost TO 1000",
        "SET search_path TO public",
        "SET application_name TO 'DEFAULT'",
        "BEGIN",
        "SET LOCAL work_mem TO DEFAULT",
        "SET LOCAL jit_above_cost = DEFAULT",
        "SET LOCAL search_path TO DEFAULT",
    ] {
        sql.execute(command).unwrap();
    }
    for (show, expected) in [
        ("SHOW work_mem", "4MB"),
        ("SHOW jit_above_cost", "100000"),
        ("SHOW search_path", "\"$user\", public"),
        ("SHOW application_name", "DEFAULT"),
    ] {
        assert_eq!(
            sql.execute(show).unwrap().rows,
            vec![vec![SqlValue::String(expected.into())]],
            "{show}"
        );
    }
    sql.execute("COMMIT").unwrap();
    for (show, expected) in [
        ("SHOW work_mem", "64MB"),
        ("SHOW jit_above_cost", "1000"),
        ("SHOW search_path", "public"),
    ] {
        assert_eq!(
            sql.execute(show).unwrap().rows,
            vec![vec![SqlValue::String(expected.into())]],
            "{show}"
        );
    }
    sql.execute("SET work_mem TO DEFAULT").unwrap();
    assert_eq!(
        sql.execute("SHOW work_mem").unwrap().rows,
        vec![vec![SqlValue::String("4MB".into())]]
    );
}

#[test]
fn search_path_preserves_quoted_names_in_session_and_persisted_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute(
            r#"CREATE SCHEMA "Partial.Path";
               CREATE SCHEMA "with,comma";
               CREATE SCHEMA "quo""te";
               CREATE TABLE public.path_probe (value text);
               SET search_path TO "Partial.Path", public;
               CREATE TABLE path_probe (value int);
               INSERT INTO path_probe VALUES (23)"#,
        )
        .unwrap();
        assert_eq!(
            sql.execute("SHOW search_path").unwrap().rows,
            [[SqlValue::String(r#""Partial.Path", public"#.into())]]
        );
        assert_eq!(
            sql.execute(r#"SELECT value FROM "Partial.Path".path_probe"#)
                .unwrap()
                .rows,
            [[SqlValue::Int(23)]]
        );
        for (assignment, name) in [
            ("SET search_path TO 'with,comma', public", "with,comma"),
            (r#"SET search_path TO "quo""te", public"#, "quo\"te"),
            (
                r#"SELECT set_config('search_path', '"with,comma", public', false)"#,
                "with,comma",
            ),
        ] {
            sql.execute(assignment).unwrap();
            assert_eq!(
                sql.execute("SELECT current_schema()").unwrap().rows,
                [[SqlValue::String(name.into())]],
                "{assignment}"
            );
        }
        sql.execute(r#"CREATE ROLE path_default_role; ALTER ROLE path_default_role SET search_path TO 'with,comma', "Partial.Path", public"#)
            .unwrap();
        let database = sql.execute("SELECT current_database()").unwrap().rows[0][0].to_cell();
        sql.execute(&format!(
            r#"ALTER DATABASE "{database}" SET search_path = 'with,comma', "Partial.Path", public"#
        ))
        .unwrap();
    }
    assert_eq!(
        bicdb_sql::role_session_settings(&db, "path_default_role").unwrap(),
        vec![(
            "search_path".into(),
            r#""with,comma", "Partial.Path", public"#.into()
        )]
    );
    let mut sql = SqlSession::new(&mut db);
    assert_eq!(
        sql.execute("SHOW search_path").unwrap().rows,
        [[SqlValue::String(
            r#""with,comma", "Partial.Path", public"#.into()
        )]]
    );
    assert_eq!(
        sql.execute("SELECT current_schema()").unwrap().rows,
        [[SqlValue::String("with,comma".into())]]
    );
}
