//! Legacy CREATE AGGREGATE syntax and aggregates over `*`.
use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

fn cell(session: &mut SqlSession<'_>, sql: &str) -> String {
    session.execute(sql).unwrap().rows[0][0].to_cell()
}

fn error(session: &mut SqlSession<'_>, sql: &str) -> (String, String) {
    let error = session.execute(sql).unwrap_err();
    (error.sqlstate().to_string(), error.to_string())
}

#[test]
fn old_syntax_and_star_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE AGGREGATE newsum (sfunc1 = int4pl, basetype = int4, stype1 = int4, initcond1 = '0')")
        .unwrap();
    assert_eq!(
        cell(
            &mut session,
            "SELECT newsum(x) FROM (VALUES (1), (2), (3)) v(x)"
        ),
        "6"
    );
    session
        .execute("CREATE AGGREGATE oldcnt (sfunc = int8inc, basetype = 'ANY', stype = int8, initcond = '0')")
        .unwrap();
    session
        .execute(
            "CREATE AGGREGATE newcnt (\"any\") (sfunc = int8inc_any, stype = int8, initcond = '0')",
        )
        .unwrap();
    assert_eq!(
        cell(
            &mut session,
            "SELECT oldcnt(*) || '/' || newcnt(x) FROM (VALUES (1), (NULL), (3)) v(x)"
        ),
        "3/2"
    );
    assert_eq!(
        error(
            &mut session,
            "CREATE AGGREGATE nb (sfunc = int4pl, stype = int4)"
        )
        .1,
        "aggregate input type must be specified"
    );
}
