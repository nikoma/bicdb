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
