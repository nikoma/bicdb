//! PostgreSQL parity for parser gaps (postfix null tests, routine DDL,
//! extended statistics, REINDEX, element assignment, split operator names,
//! JSON_TABLE clauses) and date/time input and templates. Expected values
//! are the ones PostgreSQL 18 returns; `fixtures/postgres-compat/
//! pgsql-parity/d4.cases` diffs the same statements against a live server.
use bicdb_core::{BicDb, DbConfig};
use bicdb_sql::{SqlSession, SqlValue};

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .iter()
        .map(|row| row.iter().map(SqlValue::to_cell).collect())
        .collect()
}

fn strings(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|cell| cell.to_string()).collect())
        .collect()
}

fn error(session: &mut SqlSession<'_>, sql: &str) -> (String, String) {
    match session.execute(sql) {
        Ok(_) => panic!("{sql} succeeded"),
        Err(error) => (error.sqlstate().to_string(), error.to_string()),
    }
}

fn with_session(test: impl FnOnce(&mut SqlSession<'_>)) {
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    let mut session = SqlSession::new(&mut db);
    test(&mut session);
}

#[test]
fn postfix_null_tests_collate_after_cast_and_typed_frames() {
    with_session(|s| {
        s.execute("CREATE TABLE isn(a int, b text)").unwrap();
        s.execute("INSERT INTO isn VALUES (1, NULL), (2, 'x')")
            .unwrap();
        assert_eq!(
            rows(s, "SELECT a FROM isn WHERE b isnull"),
            strings(&[&["1"]])
        );
        assert_eq!(
            rows(s, "SELECT a FROM isn WHERE b notnull"),
            strings(&[&["2"]])
        );
        assert_eq!(
            rows(
                s,
                "SELECT 'b'::text COLLATE \"C\" < 'a'::text COLLATE \"C\""
            ),
            strings(&[&["false"]])
        );
        s.execute("CREATE TABLE wif(d date, v int)").unwrap();
        s.execute("INSERT INTO wif VALUES ('2020-01-01',1),('2020-01-03',2),('2020-01-10',3)")
            .unwrap();
        assert_eq!(
            rows(
                s,
                "SELECT d, sum(v) OVER (ORDER BY d RANGE BETWEEN '3 days'::interval PRECEDING AND CURRENT ROW) FROM wif ORDER BY d"
            ),
            strings(&[&["2020-01-01", "1"], &["2020-01-03", "3"], &["2020-01-10", "3"]])
        );
    });
}

#[test]
fn routine_ddl_statistics_and_reindex() {
    with_session(|s| {
        s.execute("CREATE FUNCTION rt_f(a int) RETURNS int LANGUAGE sql AS 'SELECT a'")
            .unwrap();
        s.execute("ALTER ROUTINE rt_f(int) RENAME TO rt_f2")
            .unwrap();
        assert_eq!(rows(s, "SELECT rt_f2(5)"), strings(&[&["5"]]));
        s.execute("ALTER FUNCTION rt_f2(int) IMMUTABLE").unwrap();
        s.execute("ALTER FUNCTION rt_f2(int) STRICT").unwrap();
        assert_eq!(
            rows(
                s,
                "SELECT provolatile, proisstrict FROM pg_proc WHERE proname = 'rt_f2'"
            ),
            strings(&[&["i", "true"]])
        );
        s.execute("DROP ROUTINE rt_f2(int)").unwrap();
        let (state, message) = error(s, "DROP ROUTINE nosuch_routine(int)");
        assert_eq!(state, "42883");
        assert!(
            message.contains("function nosuch_routine(integer) does not exist"),
            "{message}"
        );

        s.execute("CREATE TABLE st1(a int, b int, c text)").unwrap();
        s.execute("CREATE STATISTICS st1_s (ndistinct, dependencies) ON a, b FROM st1")
            .unwrap();
        s.execute("CREATE STATISTICS ON a, c FROM st1").unwrap();
        assert_eq!(
            rows(
                s,
                "SELECT stxname, stxkind FROM pg_statistic_ext ORDER BY stxname"
            ),
            strings(&[
                &["st1_a_c_stat", "[\"d\",\"f\",\"m\"]"],
                &["st1_s", "[\"d\",\"f\"]"]
            ])
        );
        assert_eq!(error(s, "CREATE STATISTICS st1_x ON a FROM st1").0, "42P16");
        assert_eq!(error(s, "DROP STATISTICS nosuch_stat").0, "42704");
        s.execute("DROP STATISTICS st1_s").unwrap();

        s.execute("CREATE INDEX st1_b ON st1(b)").unwrap();
        s.execute("REINDEX INDEX st1_b").unwrap();
        s.execute("REINDEX TABLE st1").unwrap();
        assert_eq!(error(s, "REINDEX TABLE nosuch_rx").0, "42P01");
        s.execute("BEGIN").unwrap();
        assert_eq!(error(s, "REINDEX TABLE CONCURRENTLY st1").0, "25001");
        s.execute("ROLLBACK").unwrap();
        assert_eq!(
            error(
                s,
                "CREATE TABLE tmp_rng(id int, valid_at daterange, PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))"
            )
            .0,
            "0A000"
        );
    });
}

#[test]
fn element_field_and_subscript_assignment() {
    with_session(|s| {
        s.execute("CREATE TABLE aa(id int, e int[], t text[])")
            .unwrap();
        s.execute("INSERT INTO aa VALUES (1, '{1,2,3}', '{a,b}')")
            .unwrap();
        s.execute("UPDATE aa SET e[2] = 20, t[3] = 'c'").unwrap();
        s.execute("UPDATE aa SET e[1:2] = '{7,8}'").unwrap();
        assert_eq!(
            rows(s, "SELECT e, t FROM aa"),
            strings(&[&["[7,8,3]", "[\"a\",\"b\",\"c\"]"]])
        );
        s.execute("CREATE TABLE ai(id int, f int[])").unwrap();
        s.execute("INSERT INTO ai (id, f[1], f[3]) VALUES (1, 10, 30)")
            .unwrap();
        assert_eq!(rows(s, "SELECT f FROM ai"), strings(&[&["[10,null,30]"]]));
        s.execute("CREATE TYPE cpx AS (r float8, i float8)")
            .unwrap();
        s.execute("CREATE TABLE ac(id int, c cpx)").unwrap();
        s.execute("INSERT INTO ac VALUES (1, ROW(1,2))").unwrap();
        s.execute("UPDATE ac SET c.r = 9").unwrap();
        s.execute("INSERT INTO ac (id, c.i) VALUES (2, 5)").unwrap();
        assert_eq!(
            rows(s, "SELECT id, c FROM ac ORDER BY id"),
            strings(&[&["1", "(9,2)"], &["2", "(,5)"]])
        );
        s.execute("CREATE TABLE aj(j jsonb)").unwrap();
        s.execute("INSERT INTO aj VALUES ('{\"a\":1}')").unwrap();
        s.execute("UPDATE aj SET j['b'] = '2'").unwrap();
        assert_eq!(
            rows(s, "SELECT j FROM aj"),
            strings(&[&["{\"a\":1,\"b\":2}"]])
        );
    });
}

#[test]
fn split_operator_names_and_json_table_clauses() {
    with_session(|s| {
        s.execute(
            "CREATE FUNCTION op3eq(a int, b int) RETURNS bool LANGUAGE sql AS 'SELECT a = b'",
        )
        .unwrap();
        s.execute("CREATE OPERATOR === (leftarg = int, rightarg = int, function = op3eq)")
            .unwrap();
        assert_eq!(
            rows(s, "SELECT 1 === 1, 2 === 3, 3 OPERATOR(public.===) 3"),
            strings(&[&["true", "false", "true"]])
        );
        s.execute("CREATE FUNCTION optl(a text) RETURNS text LANGUAGE sql AS 'SELECT upper(a)'")
            .unwrap();
        s.execute("CREATE OPERATOR ~~~ (rightarg = text, function = optl)")
            .unwrap();
        assert_eq!(rows(s, "SELECT ~~~ 'abc'"), strings(&[&["ABC"]]));
        assert_eq!(
            rows(
                s,
                "SELECT * FROM JSON_TABLE('[1,2,3,4]'::jsonb, '$[*] ? (@ > $lim)' PASSING 2 AS lim COLUMNS (v int PATH '$'))"
            ),
            strings(&[&["3"], &["4"]])
        );
        assert_eq!(
            rows(
                s,
                "SELECT * FROM JSON_TABLE('{\"a\":[1,2],\"b\":[\"x\"]}'::jsonb, '$' COLUMNS (NESTED PATH '$.a[*]' COLUMNS (a int PATH '$'), NESTED PATH '$.b[*]' COLUMNS (b text PATH '$'))) ORDER BY a NULLS LAST, b"
            ),
            strings(&[&["1", ""], &["2", ""], &["", "x"]])
        );
    });
}

#[test]
fn datetime_special_values_zones_and_templates() {
    with_session(|s| {
        s.execute("SET timezone = 'America/Los_Angeles'").unwrap();
        assert_eq!(
            rows(
                s,
                "SELECT 'today'::date = current_date, 'tomorrow'::date - 'yesterday'::date, 'epoch'::date"
            ),
            strings(&[&["true", "2", "1970-01-01"]])
        );
        assert_eq!(
            rows(
                s,
                "SELECT 'Mon Feb 10 17:32:01 1997 PST'::timestamptz, 'Mon Feb 10 17:32:01 1997 PST'::timestamp"
            ),
            strings(&[&["1997-02-10 17:32:01-08", "1997-02-10 17:32:01"]])
        );
        assert_eq!(
            rows(s, "SELECT '1997-06-10 18:32:01 EST'::timestamptz"),
            strings(&[&["1997-06-10 16:32:01-07"]])
        );
        assert_eq!(
            rows(
                s,
                "SELECT to_timestamp('97/Feb/16', 'YY:Mon:DD'), to_date('1,582nd VIII 21', 'Y,YYYth FMRM DD')"
            ),
            strings(&[&["1997-02-16 00:00:00-08", "1582-08-21"]])
        );
        let (state, message) = error(s, "SELECT to_timestamp('97/Feb/16', 'YYMonDD')");
        assert_eq!(state, "22007");
        assert!(
            message.contains("invalid value \"/Fe\" for \"Mon\""),
            "{message}"
        );
        assert_eq!(
            rows(
                s,
                "SELECT to_char(timestamptz '2020-07-01 12:00:00-07', 'TZ OF CC FF3 IYYY-IW-ID')"
            ),
            strings(&[&["PDT -07 21 000 2020-27-3"]])
        );
        assert_eq!(
            rows(s, "SELECT to_char('infinity'::timestamp, 'YYYY') IS NULL"),
            strings(&[&["true"]])
        );
    });
}

#[test]
fn alter_operator_assigns_reciprocal_links_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session.execute("CREATE FUNCTION link_equal(int,int) RETURNS bool LANGUAGE sql IMMUTABLE RETURN $1=$2; CREATE OPERATOR === (LEFTARG=int,RIGHTARG=int,FUNCTION=link_equal); CREATE OPERATOR ==== (LEFTARG=int,RIGHTARG=int,FUNCTION=link_equal); CREATE OPERATOR ===== (LEFTARG=int,RIGHTARG=int,FUNCTION=link_equal)").unwrap();
    let links = "SELECT original.oprcom = linked.oid, linked.oprcom = original.oid, original.oprnegate = linked.oid, linked.oprnegate = original.oid FROM pg_operator AS original CROSS JOIN pg_operator AS linked WHERE original.oprname='===' AND linked.oprname='====' AND original.oprleft='integer'::regtype AND linked.oprleft='integer'::regtype";
    let assert_links = |session: &mut SqlSession<'_>, expected| {
        assert_eq!(
            session.execute(links).unwrap().rows,
            vec![vec![SqlValue::Bool(expected); 4]]
        );
    };
    let (state, message) = error(
        &mut session,
        "ALTER OPERATOR === (int,int) SET (COMMUTATOR= ====,NEGATOR= ===)",
    );
    assert_eq!(state, "42P13");
    assert!(message.contains("operator cannot be its own negator"));
    assert_links(&mut session, false);

    session
        .execute("BEGIN; ALTER OPERATOR === (int,int) SET (COMMUTATOR= ====,NEGATOR= ====)")
        .unwrap();
    assert_links(&mut session, true);
    session.execute("ROLLBACK").unwrap();
    assert_links(&mut session, false);

    session.execute("ALTER OPERATOR === (int,int) SET (COMMUTATOR= ====,NEGATOR= ====); ALTER OPERATOR === (int,int) SET (COMMUTATOR= ====,NEGATOR= ====)").unwrap();
    assert_links(&mut session, true);
    assert_eq!(
        rows(
            &mut session,
            "SELECT oprcom::regoperator::text, oprnegate::regoperator::text,
                    '===(integer,integer)'::regoperator::oid = oid,
                    'public.==='::regoper::oid = oid
             FROM pg_operator WHERE oprname = '==='"
        ),
        strings(&[&[
            "====(integer,integer)",
            "====(integer,integer)",
            "true",
            "true"
        ]])
    );
    let (state, message) = error(
        &mut session,
        "ALTER OPERATOR ===== (int,int) SET (COMMUTATOR= ===)",
    );
    assert_eq!(state, "42P13");
    assert!(message.contains("commutator operator === is already the commutator of operator ===="));
    let (state, message) = error(
        &mut session,
        "ALTER OPERATOR === (int,int) SET (NEGATOR= =====)",
    );
    assert_eq!(state, "42P13");
    assert!(message.contains("operator attribute \"negator\" cannot be changed"));
    assert_links(&mut session, true);
    session.execute("CREATE FUNCTION add_operator(int,int) RETURNS int LANGUAGE sql IMMUTABLE RETURN $1+$2; CREATE OPERATOR ~~~ (LEFTARG=int,RIGHTARG=int,FUNCTION=add_operator)").unwrap();
    let (state, message) = error(
        &mut session,
        "ALTER OPERATOR ~~~ (int,int) SET (NEGATOR= =====)",
    );
    assert_eq!(state, "42P13");
    assert!(message.contains("only boolean operators can have negators"));
}
