//! Name scoping across subquery levels: a qualified reference binds to the
//! relation it names, in the nearest scope that has it, and never to a
//! same-named column of an unrelated outer relation.
use super::*;

fn run(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> Result<SqlResult> {
    if profile == "postgres" {
        return session.execute(sql);
    }
    let sql = crate::oracle::lower_session_sql(session.db_ref(), sql, profile, None, "bicdb")?;
    session.execute(&sql)
}

fn database(profile: &str) -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
    (root, db)
}

fn cells(result: SqlResult) -> Vec<Vec<String>> {
    result
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|value| value.to_cell()).collect())
        .collect()
}

/// `o` has 200 rows whose `k` values (1001..=1200) never occur in `a`; `b`
/// is indexed on `k`, and its rows keyed by an outer `o.k` carry `v = 4`, so
/// a reference captured by the outer `o.k` changes the answer.
fn fixture(profile: &str) -> (tempfile::TempDir, BicDb) {
    let (root, mut db) = database(profile);
    let mut session = SqlSession::new(&mut db);
    for sql in [
        "CREATE TABLE o (id INTEGER PRIMARY KEY, k INTEGER)",
        "CREATE TABLE a (id INTEGER PRIMARY KEY, k INTEGER)",
        "CREATE TABLE b (id INTEGER PRIMARY KEY, k INTEGER, v INTEGER)",
        "CREATE INDEX b_k ON b (k)",
        "INSERT INTO a VALUES (1, 10), (2, 20), (3, 10)",
        "INSERT INTO b VALUES (1, 10, 1), (2, 10, 3), (3, 20, 2), (4, 1004, 4), (5, 1005, 4)",
    ] {
        run(&mut session, profile, sql).unwrap();
    }
    for id in 1..=200 {
        run(
            &mut session,
            profile,
            &format!("INSERT INTO o VALUES ({id}, {})", 1000 + id),
        )
        .unwrap();
    }
    drop(session);
    (root, db)
}

/// Runs `sql`, returning its rows and the number of full table scans.
fn profiled(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> (Vec<Vec<String>>, usize) {
    crate::SQL_PROFILE_STACK
        .with(|stack| stack.borrow_mut().push(crate::SqlProfileStats::default()));
    let result = run(session, profile, sql);
    let stats = crate::SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    (
        cells(result.unwrap_or_else(|error| panic!("{sql}: {error}"))),
        stats.full_scan_count,
    )
}

fn ids(values: &[i64]) -> Vec<Vec<String>> {
    values.iter().map(|value| vec![value.to_string()]).collect()
}

#[test]
fn inner_join_alias_is_not_captured_by_a_same_named_outer_column() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let (_root, mut db) = fixture(profile);
        let mut session = SqlSession::new(&mut db);
        // x.k = 10, so b.k = 10 yields v 1 and 3. Bound to the outer o.k it
        // would yield v = 4 for o.id 4 and 5.
        let (rows, scans) = profiled(
            &mut session,
            profile,
            "SELECT o.id FROM o WHERE o.id IN (SELECT b.v FROM a x JOIN b ON b.k = x.k WHERE x.id = 1) ORDER BY o.id",
        );
        assert_eq!(rows, ids(&[1, 3]), "{profile}");
        assert!(
            scans <= 4,
            "{profile}: uncorrelated subquery ran per row ({scans} scans)"
        );

        let (rows, _) = profiled(
            &mut session,
            profile,
            "SELECT o.id, (SELECT MAX(b.v) FROM a x JOIN b ON b.k = x.k WHERE x.id = 1) FROM o WHERE o.id IN (4, 5) ORDER BY o.id",
        );
        assert_eq!(
            rows,
            vec![
                vec!["4".to_string(), "3".to_string()],
                vec!["5".to_string(), "3".to_string()]
            ],
            "{profile}"
        );
    }
}

#[test]
fn nested_in_chain_with_an_inner_join_runs_each_level_once() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let (_root, mut db) = fixture(profile);
        let mut session = SqlSession::new(&mut db);
        // Innermost: x.k = 20 -> b2.v = 2; then b.k = 20; then a.id = 2.
        let (rows, scans) = profiled(
            &mut session,
            profile,
            "SELECT o.id FROM o WHERE o.id IN (SELECT a.id FROM a WHERE a.k IN (SELECT b.k FROM b WHERE b.v IN (SELECT b2.v FROM a x JOIN b b2 ON b2.k = x.k WHERE x.id = 2))) ORDER BY o.id",
        );
        assert_eq!(rows, ids(&[2]), "{profile}");
        // One pass per level instead of one per outer row (~200 x levels).
        assert!(
            scans <= 8,
            "{profile}: nested subqueries re-ran per row ({scans} scans)"
        );
    }
}

/// The cost-detail shape: the innermost join aliases tables (`mi m2`,
/// `po p2`) that intermediate levels read under their own names, so every
/// innermost column name also exists, differently qualified, outside.
#[test]
fn innermost_aliases_of_tables_read_by_outer_levels_stay_inner() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let (_root, mut db) = database(profile);
        let mut session = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE cd (cd_id INTEGER PRIMARY KEY, mi_id INTEGER, amt INTEGER)",
            "CREATE TABLE mi (mi_id INTEGER PRIMARY KEY, iol_id INTEGER)",
            "CREATE TABLE po (po_id INTEGER PRIMARY KEY, iol_id INTEGER, ol_id INTEGER)",
        ] {
            run(&mut session, profile, sql).unwrap();
        }
        for i in 1..=60 {
            for sql in [
                format!("INSERT INTO cd VALUES ({i}, {i}, {i})"),
                format!("INSERT INTO mi VALUES ({i}, {})", i % 5),
                format!("INSERT INTO po VALUES ({i}, {}, {i})", i % 5),
            ] {
                run(&mut session, profile, &sql).unwrap();
            }
        }
        // m2.mi_id = 7 -> iol_id 2 -> ol_id {2, 7, ..., 57} -> iol_id 2 ->
        // mi_id {2, 7, ..., 57}: 12 rows, amounts 2 + 7 + ... + 57 = 354.
        let (rows, scans) = profiled(
            &mut session,
            profile,
            "SELECT COUNT(*), SUM(amt) FROM cd WHERE mi_id IN (SELECT mi_id FROM mi WHERE iol_id IN (SELECT iol_id FROM po WHERE ol_id IN (SELECT p2.ol_id FROM mi m2 JOIN po p2 ON p2.iol_id = m2.iol_id WHERE m2.mi_id = 7)))",
        );
        assert_eq!(
            rows,
            vec![vec!["12".to_string(), "354".to_string()]],
            "{profile}"
        );
        assert!(
            scans <= 8,
            "{profile}: nested subqueries re-ran per row ({scans} scans)"
        );
    }
}

#[test]
fn correlated_references_still_bind_the_outer_row() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let (_root, mut db) = fixture(profile);
        let mut session = SqlSession::new(&mut db);
        for (sql, expected) in [
            (
                "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM b WHERE b.k = o.k AND b.v = 4) ORDER BY o.id",
                &[4, 5][..],
            ),
            (
                "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM b bb WHERE bb.k = o.k) ORDER BY o.id",
                &[4, 5],
            ),
            (
                "SELECT o.id FROM o WHERE o.k IN (SELECT bb.k FROM b bb WHERE bb.v = 4 AND bb.k = o.k) ORDER BY o.id",
                &[4, 5],
            ),
            // o.id 4 -> b.k 1004 -> v 4 (match); o.id 5 -> v 4 (no match).
            (
                "SELECT o.id FROM o WHERE o.id IN (SELECT b.v FROM a x JOIN b ON b.k = o.k WHERE x.id = 1) ORDER BY o.id",
                &[4],
            ),
        ] {
            assert_eq!(
                profiled(&mut session, profile, sql).0,
                ids(expected),
                "{profile}: {sql}"
            );
        }
        // Unqualified names still reach the outer scope when no inner relation has them.
        assert_eq!(
            profiled(&mut session, profile, "SELECT o.id FROM o WHERE o.id < 3 AND EXISTS (SELECT 1 FROM a x WHERE x.id = o.id) ORDER BY o.id").0,
            ids(&[1, 2]),
            "{profile}"
        );
    }
}

#[test]
fn an_inner_alias_shadows_the_outer_relation_of_the_same_name() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let (_root, mut db) = fixture(profile);
        let mut session = SqlSession::new(&mut db);
        let (rows, scans) = profiled(
            &mut session,
            profile,
            "SELECT o.id FROM o WHERE o.id <= 3 AND EXISTS (SELECT 1 FROM a o WHERE o.k = 20) ORDER BY o.id",
        );
        assert_eq!(rows, ids(&[1, 2, 3]), "{profile}");
        assert!(
            scans <= 3,
            "{profile}: shadowed subquery treated as correlated ({scans} scans)"
        );
        assert!(
            profiled(
                &mut session,
                profile,
                "SELECT o.id FROM o WHERE EXISTS (SELECT 1 FROM a o WHERE o.k = 1001)"
            )
            .0
            .is_empty(),
            "{profile}"
        );
    }
}

#[test]
fn duplicate_outer_column_names_bind_the_named_relation() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let (_root, mut db) = fixture(profile);
        let mut session = SqlSession::new(&mut db);
        // a.k is 10, 20, 10 for o.id 1..3; b has (k 10, v 3).
        assert_eq!(
            profiled(&mut session, profile, "SELECT o.id FROM o JOIN a ON a.id = o.id WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.v = 3) ORDER BY o.id").0,
            ids(&[1, 3]),
            "{profile}"
        );
        assert!(
            profiled(&mut session, profile, "SELECT o.id FROM o JOIN a ON a.id = o.id WHERE EXISTS (SELECT 1 FROM b WHERE b.k = o.k AND b.v = 3)").0.is_empty(),
            "{profile}"
        );
        // An inner alias over a relation with the same column name as both outer ones.
        assert_eq!(
            profiled(&mut session, profile, "SELECT o.id FROM o JOIN a ON a.id = o.id WHERE o.id IN (SELECT y.v FROM b y WHERE y.k = 10) ORDER BY o.id").0,
            ids(&[1, 3]),
            "{profile}"
        );
    }
}
