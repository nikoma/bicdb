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
