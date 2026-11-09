//! Boundary regressions for declared comparison types. Expected results were
//! checked against PostgreSQL 16.13 using the same SQL.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{infer_query_result_types, SqlSession, SqlValue};

fn database() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    (root, db)
}

fn cells(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| match v {
                    SqlValue::Null => "<null>".to_string(),
                    value => value.to_cell(),
                })
                .collect()
        })
        .collect()
}

#[test]
fn quoted_column_identities_keep_their_own_declared_types() {
    let (_root, mut db) = database();
    {
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE identity_rows (\"K\" numeric,k float8,\"P\" char(4),p text)",
            "INSERT INTO identity_rows VALUES(9007199254740993,9007199254740992,'ab','ab ')",
            "CREATE VIEW identity_view AS SELECT \"K\",k,\"P\",p FROM identity_rows",
            "CREATE FUNCTION identity_record() RETURNS text LANGUAGE plpgsql AS $$DECLARE r record; BEGIN FOR r IN SELECT \"K\",k FROM identity_rows LOOP RETURN (r.\"K\"=r.k)::text; END LOOP; RETURN 'none'; END$$",
        ] { session.execute(sql).unwrap(); }
    }
    for sql in [
        "SELECT \"K\",k FROM identity_rows",
        "SELECT q.\"K\",q.k FROM (SELECT \"K\",k FROM identity_rows) q",
        "WITH q AS (SELECT \"K\",k FROM identity_rows) SELECT q.\"K\",q.k FROM q",
        "SELECT \"K\",k FROM identity_view",
    ] {
        assert_eq!(
            infer_query_result_types(&db, sql).unwrap().unwrap(),
            vec![Some("numeric".to_string()), Some("float8".to_string())],
            "{sql}"
        );
        assert_eq!(
            SqlSession::new(&mut db).execute(sql).unwrap().column_types,
            vec![Some("numeric".to_string()), Some("float8".to_string())],
            "{sql}"
        );
    }
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "SELECT \"K\"=k,\"P\"=p FROM identity_rows",
        "SELECT q.\"K\"=q.k,q.\"P\"=q.p FROM (SELECT \"K\",k,\"P\",p FROM identity_rows) q",
        "WITH q AS (SELECT \"K\",k,\"P\",p FROM identity_rows) SELECT q.\"K\"=q.k,q.\"P\"=q.p FROM q",
        "SELECT \"K\"=k,\"P\"=p FROM identity_view",
        "SELECT q.\"K\"=q.k,q.\"P\"=q.p FROM (SELECT \"K\",k,\"P\",p FROM identity_rows) q(\"K\",k,\"P\",p)",
    ] { assert_eq!(cells(&mut session,sql), vec![vec!["true","false"]], "{sql}"); }
    assert_eq!(
        cells(
            &mut session,
            "SELECT q.K=q.k,q.P=q.p FROM (SELECT \"K\",k,\"P\",p FROM identity_rows) q"
        ),
        vec![vec!["true", "true"]]
    );
    assert_eq!(
        cells(&mut session, "SELECT identity_record()"),
        vec![vec!["true"]]
    );
    assert_eq!(cells(&mut session,"SELECT q.\"a.b\"=q.c FROM (SELECT 9007199254740993::numeric AS \"a.b\",9007199254740992::float8 AS c) q"),vec![vec!["true"]]);
    session
        .execute("CREATE TABLE wide_keys(k bigint PRIMARY KEY)")
        .unwrap();
    session
        .execute("INSERT INTO wide_keys VALUES(9007199254740992),(9007199254740993)")
        .unwrap();
    assert_eq!(cells(&mut session,"SELECT w.k FROM (SELECT 9007199254740992::float8 AS \"a.b\") p JOIN wide_keys w ON w.k=p.\"a.b\" ORDER BY w.k"),vec![vec!["9007199254740992"],vec!["9007199254740993"]]);
}

#[test]
fn unknown_null_and_array_operands_retain_sql_null_behavior() {
    let (_root, mut db) = database();
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TYPE boundary_mood AS ENUM('ok','happy')",
        "CREATE DOMAIN boundary_label AS text",
        "CREATE TABLE boundary_rows(id int,d date,t timestamp,n numeric,i int,a int[],m boundary_mood,label boundary_label)",
        "INSERT INTO boundary_rows VALUES(1,'2002-07-04','2002-07-04 00:00:00',2.00,1,ARRAY[1,2],'ok','ok'),(2,NULL,NULL,NULL,NULL,ARRAY[NULL]::int[],NULL,NULL)",
    ] { session.execute(sql).unwrap(); }
    for source in ["boundary_rows", "(SELECT * FROM boundary_rows) q"] {
        let sql=format!("SELECT CASE m WHEN 'ok' THEN true ELSE false END,CASE label WHEN 'ok' THEN true ELSE false END,CASE a WHEN '{{1,2}}' THEN true ELSE false END FROM {source} WHERE id=1");
        assert_eq!(
            cells(&mut session, &sql),
            vec![vec!["true", "true", "true"]]
        );
    }
    assert_eq!(
        cells(
            &mut session,
            "SELECT d=NULL,NULL=t,n=NULL,NULL=i,a=NULL,NULL=m FROM boundary_rows ORDER BY id"
        ),
        vec![vec!["<null>"; 6]; 2]
    );
    assert_eq!(
        cells(
            &mut session,
            "SELECT d=t,NOT(d=t),(d=t) OR true,(d=t) AND false FROM boundary_rows WHERE id=2"
        ),
        vec![vec!["<null>", "<null>", "true", "false"]]
    );
    assert_eq!(cells(&mut session,"SELECT a=ARRAY[NULL]::int[],CASE m WHEN NULL THEN 'bad' ELSE 'null' END FROM boundary_rows WHERE id=2"),vec![vec!["true","null"]]);
}

#[test]
fn query_type_frames_follow_alias_scopes_and_unwind_after_errors() {
    let (_root, mut db) = database();
    let mut session = SqlSession::new(&mut db);
    let temporal="SELECT q.a=q.b FROM (SELECT date '2002-07-04' AS a,timestamp '2002-07-04 00:00:00' AS b) q";
    for sql in [temporal,"SELECT q.a=q.b FROM (SELECT 9007199254740993::numeric AS a,9007199254740992::float8 AS b) q",temporal] {
        assert_eq!(cells(&mut session,sql),vec![vec!["true"]]);
    }
    assert_eq!(
        session
            .execute("SELECT 'bad'::int FROM (SELECT 'a' AS a,'b' AS b) q")
            .unwrap_err()
            .sqlstate(),
        "22P02"
    );
    assert_eq!(cells(&mut session, temporal), vec![vec!["true"]]);
    for sql in [
        "CREATE SCHEMA first_scope",
        "CREATE SCHEMA second_scope",
        "CREATE TABLE first_scope.same(v date)",
        "CREATE TABLE second_scope.same(v timestamp)",
        "INSERT INTO first_scope.same VALUES('2002-07-04')",
        "INSERT INTO second_scope.same VALUES('2002-07-04 00:00:00')",
        "CREATE TABLE shadow_rows(id int,d date,t timestamp)",
        "INSERT INTO shadow_rows VALUES(1,'2002-07-04','2002-07-04 00:00:00'),(2,NULL,NULL)",
    ] {
        session.execute(sql).unwrap();
    }
    assert_eq!(
        cells(
            &mut session,
            "SELECT x.v=y.v FROM first_scope.same x JOIN second_scope.same y ON true"
        ),
        vec![vec!["true"]]
    );
    assert_eq!(cells(&mut session,"SELECT count(*) FROM shadow_rows o WHERE EXISTS(SELECT 1 FROM (SELECT d AS x,t AS y FROM shadow_rows WHERE id=1) o WHERE o.x=o.y)"),vec![vec!["2"]]);
}

#[test]
fn comparison_projections_keep_explicit_collation_ordering() {
    let (_root, mut db) = database();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE collation_rows(id int,c text COLLATE \"C\",e text COLLATE \"en-x-icu\")",
        )
        .unwrap();
    session
        .execute("INSERT INTO collation_rows VALUES(1,'a','Z'),(2,'Z','a')")
        .unwrap();
    assert_eq!(
        cells(
            &mut session,
            "SELECT c COLLATE \"en-x-icu\"<e FROM collation_rows ORDER BY id"
        ),
        vec![vec!["true"], vec!["false"]]
    );
    assert_eq!(
        cells(
            &mut session,
            "SELECT id FROM collation_rows WHERE c<e COLLATE \"C\" ORDER BY id"
        ),
        vec![vec!["2"]]
    );
}
