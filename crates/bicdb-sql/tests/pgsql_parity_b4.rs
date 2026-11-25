use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn int(value: i64) -> SqlValue {
    SqlValue::Int(value)
}

fn text(value: &str) -> SqlValue {
    SqlValue::String(value.to_string())
}

fn cells(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<String>> {
    session
        .execute(sql)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|value| value.to_cell()).collect())
        .collect()
}

fn rows(session: &mut SqlSession<'_>, sql: &str) -> Vec<Vec<SqlValue>> {
    session.execute(sql).unwrap().rows
}

#[test]
fn statistics_aggregates_match_postgresql_values_and_types() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE sv(a int, d float8, s int2); \
             INSERT INTO sv VALUES (1, 1.5, 1), (2, 2.25, 2), (4, 3.125, 4), (NULL, NULL, NULL)",
        )
        .unwrap();
    assert_eq!(
        cells(
            &mut session,
            "SELECT variance(a), var_pop(a), stddev(a), stddev_pop(a) FROM sv"
        ),
        vec![vec![
            "2.3333333333333333".to_string(),
            "1.5555555555555556".to_string(),
            "1.5275252316519467".to_string(),
            "1.2472191289246471".to_string(),
        ]]
    );
    let result = session
        .execute("SELECT variance(a), stddev(d), bit_and(s), regr_count(a, d), corr(a, d) FROM sv")
        .unwrap();
    let types: Vec<Option<&str>> = result.column_types.iter().map(Option::as_deref).collect();
    assert_eq!(
        types,
        vec![
            Some("numeric"),
            Some("float8"),
            Some("int2"),
            Some("int8"),
            Some("float8")
        ]
    );
    assert_eq!(
        cells(
            &mut session,
            "SELECT pg_typeof(stddev(d)), pg_typeof(bit_or(s)), pg_typeof(var_pop(a)) FROM sv"
        ),
        vec![vec![
            "double precision".to_string(),
            "smallint".to_string(),
            "numeric".to_string(),
        ]]
    );
    // One row: the sample statistics are NULL, the population ones zero.
    assert_eq!(
        cells(
            &mut session,
            "SELECT var_samp(a), var_pop(a), covar_samp(a, a), covar_pop(a, a) FROM sv WHERE a = 1"
        ),
        vec![vec![
            "".to_string(),
            "0".to_string(),
            "".to_string(),
            "0".to_string()
        ]]
    );
}

#[test]
fn regression_and_bit_aggregates() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE sr(y float8, x float8); \
             INSERT INTO sr VALUES (1, 2), (3, 5), (4, 4), (7, 9), (NULL, 1), (2, NULL); \
             CREATE TABLE sb(a int); INSERT INTO sb VALUES (12), (10), (NULL)",
        )
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT regr_count(y, x), regr_sxx(y, x), regr_syy(y, x), regr_sxy(y, x), \
             regr_avgx(y, x), regr_avgy(y, x) FROM sr"
        ),
        vec![vec![
            int(4),
            SqlValue::Float(26.0),
            SqlValue::Float(18.75),
            SqlValue::Float(21.0),
            SqlValue::Float(5.0),
            SqlValue::Float(3.75),
        ]]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT bit_and(a), bit_or(a), bit_xor(a) FROM sb"
        ),
        vec![vec![int(8), int(14), int(6)]]
    );
    assert_eq!(
        rows(&mut session, "SELECT bit_and(a) FROM sb WHERE false"),
        vec![vec![SqlValue::Null]]
    );
}

#[test]
fn aggregates_without_from_read_one_row() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        cells(
            &mut session,
            "SELECT sum(1), count(*), max('x'), var_pop(1.0::float8), var_samp(2::numeric)"
        ),
        vec![vec![
            "1".to_string(),
            "1".to_string(),
            "x".to_string(),
            "0".to_string(),
            "".to_string(),
        ]]
    );
}

#[test]
fn make_date_time_and_unknown_literals() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        cells(
            &mut session,
            "SELECT make_date(2024, 2, 29), make_time(8, 15, 23.5), \
             make_timestamp(2013, 7, 15, 8, 15, 23.5), make_date(-44, 3, 15)"
        ),
        vec![vec![
            "2024-02-29".to_string(),
            "08:15:23.5".to_string(),
            "2013-07-15 08:15:23.5".to_string(),
            "0044-03-15 BC".to_string(),
        ]]
    );
    let error = session
        .execute("SELECT make_date(2013, 2, 30)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "22008");
    assert_eq!(
        error.to_string(),
        "date field value out of range: 2013-02-30"
    );
}

#[test]
fn partition_info_functions_and_multilevel_partitions() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE pml(a int, b int) PARTITION BY RANGE (a); \
             CREATE TABLE pml1 PARTITION OF pml FOR VALUES FROM (0) TO (10) PARTITION BY LIST (b); \
             CREATE TABLE pml1a PARTITION OF pml1 FOR VALUES IN (1, 2); \
             CREATE TABLE pml_def PARTITION OF pml DEFAULT PARTITION BY RANGE (b); \
             CREATE TABLE pml_def1 PARTITION OF pml_def FOR VALUES FROM (0) TO (100); \
             INSERT INTO pml VALUES (5, 1), (50, 50)",
        )
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a, b FROM pml1a"),
        vec![vec![int(5), int(1)]]
    );
    assert_eq!(
        rows(&mut session, "SELECT a, b FROM pml_def1"),
        vec![vec![int(50), int(50)]]
    );
    let error = session
        .execute("INSERT INTO pml VALUES (5, 3)")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "23514");
    assert_eq!(
        cells(
            &mut session,
            "SELECT relid, parentrelid, isleaf, level FROM pg_partition_tree('pml')"
        ),
        vec![
            vec!["pml", "", "false", "0"],
            vec!["pml1", "pml", "false", "1"],
            vec!["pml_def", "pml", "false", "1"],
            vec!["pml1a", "pml1", "true", "2"],
            vec!["pml_def1", "pml_def", "true", "2"],
        ]
        .into_iter()
        .map(|row| row.into_iter().map(str::to_string).collect::<Vec<_>>())
        .collect::<Vec<_>>()
    );
    assert_eq!(
        cells(
            &mut session,
            "SELECT * FROM pg_partition_ancestors('pml1a')"
        ),
        vec![vec!["pml1a"], vec!["pml1"], vec!["pml"]]
            .into_iter()
            .map(|row| row.into_iter().map(str::to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT pg_partition_root('pml1a')::text, pg_partition_root('pml')::text"
        ),
        vec![vec![text("pml"), text("pml")]]
    );
    session.execute("CREATE TABLE plain(a int)").unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT count(*) FROM pg_partition_tree('plain')"
        ),
        vec![vec![int(0)]]
    );
}

#[test]
fn partition_attach_matches_columns_by_name_with_postgresql_errors() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE pao(a int, b text) PARTITION BY LIST (a); \
             CREATE TABLE pao1(b text, a int); \
             ALTER TABLE pao ATTACH PARTITION pao1 FOR VALUES IN (1); \
             INSERT INTO pao VALUES (1, 'x')",
        )
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM pao"),
        vec![vec![int(1), text("x")]]
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM pao1"),
        vec![vec![text("x"), int(1)]]
    );
    session
        .execute(
            "CREATE TABLE pae_extra(a int, b text, c int); CREATE TABLE pae_missing(a int); \
             CREATE TABLE pae_type(a int, b varchar); CREATE TABLE pae_rows(a int, b text); \
             INSERT INTO pae_rows VALUES (7, 'z')",
        )
        .unwrap();
    for (sql, message) in [
        (
            "ALTER TABLE pao ATTACH PARTITION pae_extra FOR VALUES IN (2)",
            "table \"pae_extra\" contains column \"c\" not found in parent \"pao\"",
        ),
        (
            "ALTER TABLE pao ATTACH PARTITION pae_missing FOR VALUES IN (2)",
            "child table is missing column \"b\"",
        ),
        (
            "ALTER TABLE pao ATTACH PARTITION pae_type FOR VALUES IN (2)",
            "child table \"pae_type\" has different type for column \"b\"",
        ),
        (
            "ALTER TABLE pao ATTACH PARTITION pae_rows FOR VALUES IN (2)",
            "partition constraint of relation \"pae_rows\" is violated by some row",
        ),
        (
            "CREATE TABLE pao_overlap PARTITION OF pao FOR VALUES IN (1)",
            "partition \"pao_overlap\" would overlap partition \"pao1\"",
        ),
        (
            "CREATE TABLE pao_bad PARTITION OF pao FOR VALUES IN (somename)",
            "cannot use column reference in partition bound expression",
        ),
        (
            "CREATE TABLE pao_bad PARTITION OF pao FOR VALUES IN (sum(1))",
            "aggregate functions are not allowed in partition bound",
        ),
    ] {
        let error = session.execute(sql).unwrap_err().to_string();
        assert!(error.ends_with(message), "{sql}: {error}");
    }
}

#[test]
fn lock_and_truncate_follow_postgresql_privileges() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE ROLE ins; CREATE ROLE sel; CREATE ROLE nobody; CREATE ROLE trunc; \
             CREATE TABLE lt(a int); INSERT INTO lt VALUES (1); \
             GRANT INSERT ON lt TO ins; GRANT SELECT ON lt TO sel; \
             GRANT SELECT, DELETE ON lt TO nobody; GRANT TRUNCATE ON lt TO trunc",
        )
        .unwrap();
    session.execute("SET ROLE ins").unwrap();
    session
        .execute("BEGIN; LOCK TABLE lt IN ROW EXCLUSIVE MODE; COMMIT")
        .unwrap();
    // Authorization canaries: INSERT does not reach the strong modes.
    session.execute("BEGIN").unwrap();
    let error = session
        .execute("LOCK TABLE lt IN ACCESS EXCLUSIVE MODE")
        .unwrap_err();
    assert!(error
        .to_string()
        .ends_with("permission denied for table lt"));
    session.execute("ROLLBACK").unwrap();
    session.execute("RESET ROLE; SET ROLE sel").unwrap();
    session
        .execute("BEGIN; LOCK TABLE lt IN ACCESS SHARE MODE; COMMIT")
        .unwrap();
    session.execute("BEGIN").unwrap();
    assert!(session
        .execute("LOCK TABLE lt IN ROW EXCLUSIVE MODE")
        .is_err());
    session.execute("ROLLBACK").unwrap();
    assert!(session.execute("TRUNCATE lt").is_err());
    session.execute("RESET ROLE; SET ROLE nobody").unwrap();
    // DELETE permits every lock mode, but not TRUNCATE.
    session
        .execute("BEGIN; LOCK TABLE lt IN ACCESS EXCLUSIVE MODE; COMMIT")
        .unwrap();
    let error = session.execute("TRUNCATE lt").unwrap_err();
    assert!(error
        .to_string()
        .ends_with("permission denied for table lt"));
    session.execute("RESET ROLE; SET ROLE trunc").unwrap();
    session.execute("TRUNCATE lt").unwrap();
    session.execute("RESET ROLE").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM lt"),
        vec![vec![int(0)]]
    );
}

#[test]
fn admin_option_holders_manage_role_membership_and_nobody_else() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE ROLE grp; CREATE ROLE adm; CREATE ROLE via; CREATE ROLE plain; \
             CREATE ROLE target; GRANT grp TO adm WITH ADMIN OPTION; GRANT adm TO via",
        )
        .unwrap();
    session.execute("SET ROLE adm").unwrap();
    session.execute("GRANT grp TO target").unwrap();
    session.execute("REVOKE grp FROM target").unwrap();
    session.execute("RESET ROLE; SET ROLE via").unwrap();
    // ADMIN OPTION reached through a membership chain counts, as in PostgreSQL.
    session.execute("GRANT grp TO target").unwrap();
    // Authorization canaries: no ADMIN OPTION, or the role itself, is refused.
    session.execute("RESET ROLE; SET ROLE plain").unwrap();
    let error = session.execute("GRANT grp TO plain").unwrap_err();
    assert_eq!(error.sqlstate(), "42501");
    assert!(error
        .to_string()
        .ends_with("permission denied to grant role \"grp\""));
    assert!(session.execute("REVOKE grp FROM target").is_err());
    assert!(session.execute("GRANT adm TO plain").is_err());
    session.execute("RESET ROLE; GRANT grp TO plain").unwrap();
    session.execute("SET ROLE grp").unwrap();
    assert!(session.execute("GRANT grp TO via").is_err());
    session.execute("RESET ROLE").unwrap();
    session.execute("SET ROLE adm").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT current_role"),
        vec![vec![text("adm")]]
    );
}
