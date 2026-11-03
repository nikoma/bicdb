//! Native PostgreSQL declaration metadata controls, independent of Oracle lowering.
use crate::*;

#[test]
fn native_function_attributes_keep_catalog_values_through_replace_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE FUNCTION attr_immutable() RETURNS INT IMMUTABLE LANGUAGE sql AS $$SELECT 1$$",
            "CREATE FUNCTION attr_stable() RETURNS INT STABLE LANGUAGE sql AS $$SELECT 2$$",
            "CREATE FUNCTION attr_volatile() RETURNS INT VOLATILE LANGUAGE sql AS $$SELECT 3$$",
            "CREATE FUNCTION attr_default(\"IMMUTABLE\" INT) RETURNS TEXT LANGUAGE sql AS $$SELECT 'STABLE VOLATILE IMMUTABLE'$$",
            "CREATE PROCEDURE attr_procedure() LANGUAGE plpgsql AS $$BEGIN NULL;END$$",
        ] {
            session.execute(sql).unwrap();
        }
    }
    let expected = vec![
        vec!["attr_default", "v"],
        vec!["attr_immutable", "i"],
        vec!["attr_procedure", "v"],
        vec!["attr_stable", "s"],
        vec!["attr_volatile", "v"],
    ];
    let query =
        "SELECT proname,provolatile FROM pg_proc WHERE proname LIKE 'attr_%' ORDER BY proname";
    let rows = |result: SqlResult| {
        result
            .rows
            .iter()
            .map(|row| row.iter().map(SqlValue::to_cell).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        rows(SqlSession::new(&mut db).execute(query).unwrap()),
        expected
    );
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    assert_eq!(
        rows(SqlSession::new(&mut db).execute(query).unwrap()),
        expected
    );
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE OR REPLACE FUNCTION attr_immutable() RETURNS INT STABLE LANGUAGE sql AS $$SELECT 42$$").unwrap();
    assert_eq!(
        rows(
            session
                .execute("SELECT provolatile FROM pg_proc WHERE proname='attr_immutable'")
                .unwrap()
        ),
        vec![vec!["s"]]
    );
    assert_eq!(
        session.execute("SELECT attr_immutable()").unwrap().rows,
        vec![vec![SqlValue::Int(42)]]
    );
}

fn security_marker_fixture(db: &mut BicDb) {
    let mut session = SqlSession::new(db);
    for sql in [
        "CREATE ROLE marker_caller",
        "GRANT USAGE ON SCHEMA public TO marker_caller",
        "CREATE TABLE marker_private(secret INT)",
        "INSERT INTO marker_private VALUES(7)",
        "REVOKE ALL ON TABLE marker_private FROM PUBLIC",
        "CREATE FUNCTION marker_default() RETURNS INT LANGUAGE plpgsql AS $$DECLARE marker TEXT:='SECURITY DEFINER';v INT;BEGIN SELECT secret INTO v FROM marker_private;RETURN v;END$$",
        "CREATE FUNCTION marker_invoker() RETURNS INT LANGUAGE plpgsql SECURITY INVOKER AS $$DECLARE marker TEXT:='SECURITY DEFINER';v INT;BEGIN SELECT secret INTO v FROM marker_private;RETURN v;END$$",
        "CREATE FUNCTION marker_definer() RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS $$DECLARE v INT;BEGIN SELECT secret INTO v FROM marker_private;RETURN v;END$$",
        "GRANT EXECUTE ON FUNCTION marker_default() TO marker_caller",
        "GRANT EXECUTE ON FUNCTION marker_invoker() TO marker_caller",
        "GRANT EXECUTE ON FUNCTION marker_definer() TO marker_caller",
    ] {
        session.execute(sql).unwrap();
    }
}

#[test]
fn body_security_marker_preserves_invoker_permissions_and_explicit_definer_access() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    security_marker_fixture(&mut db);
    let mut session = SqlSession::new(&mut db);
    session.execute("SET ROLE marker_caller").unwrap();
    for sql in ["SELECT marker_default()", "SELECT marker_invoker()"] {
        let error = session.execute(sql).unwrap_err();
        assert_eq!(error.sqlstate(), "42501", "{sql}: {error}");
    }
    assert_eq!(
        session.execute("SELECT marker_definer()").unwrap().rows,
        vec![vec![SqlValue::Int(7)]]
    );
}

#[test]
fn legacy_body_security_marker_flags_are_recomputed_from_the_declaration() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    security_marker_fixture(&mut db);
    let mut legacy = load_routine(&db, RoutineKind::Function, "marker_default")
        .unwrap()
        .unwrap();
    legacy.security_definer = true;
    save_routine(&mut db, &legacy).unwrap();
    drop(db);
    let mut db = BicDb::open(root.path()).unwrap();
    let routine = load_routine(&db, RoutineKind::Function, "marker_default")
        .unwrap()
        .unwrap();
    assert!(!routine.security_definer);
    let mut session = SqlSession::new(&mut db);
    session.execute("SET ROLE marker_caller").unwrap();
    assert_eq!(
        session
            .execute("SELECT marker_default()")
            .unwrap_err()
            .sqlstate(),
        "42501"
    );
    assert_eq!(
        session.execute("SELECT marker_definer()").unwrap().rows,
        vec![vec![SqlValue::Int(7)]]
    );
}

#[test]
fn declaration_security_attributes_exclude_body_literals_and_argument_defaults() {
    for (definition, expected) in [
        ("CREATE FUNCTION f() RETURNS TEXT LANGUAGE sql AS $$SELECT 'SECURITY DEFINER'$$", false),
        ("CREATE FUNCTION f(p TEXT DEFAULT 'SECURITY DEFINER') RETURNS TEXT LANGUAGE sql AS $$SELECT p$$", false),
        ("CREATE FUNCTION f() RETURNS TEXT LANGUAGE sql SECURITY INVOKER AS $$SELECT 'SECURITY DEFINER'$$", false),
        ("CREATE FUNCTION f() RETURNS TEXT LANGUAGE sql SECURITY /* header comment */ DEFINER AS $$SELECT 'safe'$$", true),
        ("CREATE PROCEDURE p() LANGUAGE plpgsql AS $$BEGIN RAISE NOTICE 'SECURITY DEFINER';END$$", false),
        ("CREATE PROCEDURE p() LANGUAGE plpgsql SECURITY INVOKER AS $$BEGIN RAISE NOTICE 'SECURITY DEFINER';END$$", false),
        ("CREATE PROCEDURE p() LANGUAGE plpgsql SECURITY DEFINER AS $$BEGIN NULL;END$$", true),
        ("CREATE PROCEDURE p() AS $$BEGIN NULL;END$$ LANGUAGE plpgsql SECURITY DEFINER", true),
        ("SELECT 'SECURITY DEFINER'", false),
    ] {
        assert_eq!(routine_definition_is_security_definer(definition), expected, "{definition}");
    }
}
