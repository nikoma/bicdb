//! `upper(col)` / `lower(col)` equality must be answered by an expression
//! index on that function (the application dictionary's unique
//! `UPPER(TableName)` keys), never by a scan of the table, and must return
//! exactly the rows the unindexed predicate returns, non-ASCII text included.
use super::*;

fn run(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> Result<SqlResult> {
    if profile == "postgres" {
        return session.execute(sql);
    }
    let sql = crate::oracle::lower_session_sql(session.db_ref(), sql, profile, None, "bicdb")?;
    session.execute(&sql)
}

/// (rows as text, full scans, index lookups)
fn profiled(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> (Vec<String>, usize, usize) {
    crate::SQL_PROFILE_STACK
        .with(|stack| stack.borrow_mut().push(crate::SqlProfileStats::default()));
    let result = run(session, profile, sql);
    let stats = crate::SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    let rows = result
        .unwrap_or_else(|error| panic!("{profile}: {sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| value.to_cell())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    (rows, stats.full_scan_count, stats.index_lookup_count)
}

const SPECIAL_ROWS: &[(i64, &str)] = &[
    (1, "'Straße'"),
    (2, "'STRASSE'"),
    (3, "'straße'"),
    (4, "'İstanbul'"),
    (5, "'istanbul'"),
    (6, "'ǆungla'"),
    (7, "'Ǆungla'"),
    (8, "'ǅungla'"),
    (9, "'AD_Table'"),
    (10, "'ad_table'"),
    (11, "'ıd'"),
    (12, "NULL"),
];

/// Every query is answerable from one `upper(name_text)`/`lower(name_text)` index.
/// The expected labels are PostgreSQL's under the C LC_CTYPE BicDB reports
/// (ASCII-only case mapping) for queries marked `Some`; queries whose answer
/// depends on non-ASCII case mapping (`None`) are compared with the
/// unindexed result of the same profile instead.
fn queries() -> Vec<(&'static str, Option<Vec<&'static str>>)> {
    vec![
        (
            "upper(name_text) = 'AD_TABLE'",
            Some(vec!["AD_Table", "ad_table"]),
        ),
        (
            "'AD_TABLE' = upper(name_text)",
            Some(vec!["AD_Table", "ad_table"]),
        ),
        (
            "upper(name_text) = upper('ad_Table')",
            Some(vec!["AD_Table", "ad_table"]),
        ),
        (
            "UPPER(dict_names.name_text) = 'AD_TABLE'",
            Some(vec!["AD_Table", "ad_table"]),
        ),
        (
            "lower(name_text) = 'ad_table'",
            Some(vec!["AD_Table", "ad_table"]),
        ),
        (
            "upper(name_text) = 'STRAßE'",
            Some(vec!["Straße", "straße"]),
        ),
        ("upper(name_text) = 'STRASSE'", Some(vec!["STRASSE"])),
        (
            "lower(name_text) = 'straße'",
            Some(vec!["Straße", "straße"]),
        ),
        ("upper(name_text) = 'ISTANBUL'", Some(vec!["istanbul"])),
        ("upper(name_text) = 'NAME77'", Some(vec!["Name77"])),
        ("upper(name_text) = 'ABSENT'", Some(vec![])),
        ("upper(name_text) = 'ǄUNGLA'", None),
        ("upper(name_text) = 'İSTANBUL'", None),
        ("upper(name_text) = 'ID'", None),
        ("lower(name_text) = 'ǆungla'", None),
        ("lower(name_text) = 'istanbul'", None),
        ("lower(name_text) = 'i̇stanbul'", None),
        (
            "tenant = 7 AND upper(name_text) = 'AD_TABLE'",
            Some(vec!["AD_Table"]),
        ),
    ]
}

fn select(predicate: &str) -> String {
    format!("SELECT name_text FROM dict_names WHERE {predicate} ORDER BY id")
}

#[test]
fn case_mapped_equality_uses_expression_indexes_on_reopened_paged_tables() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let root = tempfile::tempdir().unwrap();
        let config = bicdb_core::DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(bicdb_core::StorageMode::ServerPaged);
        let (key, text) = if profile == "postgres" {
            ("NUMERIC(10)", "VARCHAR(40)")
        } else {
            ("NUMBER(10)", "VARCHAR2(40)")
        };
        let unindexed: Vec<Vec<String>>;
        {
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
            let mut session = SqlSession::new(&mut db);
            run(
                &mut session,
                profile,
                &format!("CREATE TABLE dict_names (id {key} PRIMARY KEY, tenant {key}, name_text {text})"),
            )
            .unwrap();
            for (id, name_text) in SPECIAL_ROWS {
                let tenant = if *id == 9 { 7 } else { 8 };
                run(
                    &mut session,
                    profile,
                    &format!("INSERT INTO dict_names VALUES ({id}, {tenant}, {name_text})"),
                )
                .unwrap();
            }
            for id in 100..500 {
                run(
                    &mut session,
                    profile,
                    &format!("INSERT INTO dict_names VALUES ({id}, 7, 'Name{}')", id - 23),
                )
                .unwrap();
            }
            // The same predicates before any expression index exists.
            unindexed = queries()
                .iter()
                .map(|(predicate, _)| profiled(&mut session, profile, &select(predicate)).0)
                .collect();
            for sql in [
                "CREATE INDEX dict_names_upper ON dict_names (UPPER(name_text))",
                "CREATE INDEX dict_names_lower ON dict_names (LOWER(name_text))",
                "CREATE INDEX dict_names_tenant_upper ON dict_names (tenant, UPPER(name_text))",
            ] {
                run(&mut session, profile, sql).unwrap();
            }
            drop(session);
            db.close().unwrap();
        }
        // Reopen: the table is now lazy, as in every long-lived server-paged
        // database.
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
        let mut session = SqlSession::new(&mut db);
        for ((predicate, expected), unindexed) in queries().into_iter().zip(&unindexed) {
            let sql = select(predicate);
            let (rows, scans, lookups) = profiled(&mut session, profile, &sql);
            assert_eq!(
                &rows, unindexed,
                "{profile}: {sql}: index changed the result"
            );
            // These answers are the same under ASCII and Unicode case mapping.
            if let Some(expected) = &expected {
                assert_eq!(rows, *expected, "{profile}: {sql}");
            }
            assert_eq!(scans, 0, "{profile}: {sql}: scanned the table");
            assert!(lookups > 0, "{profile}: {sql}: no index lookup");
        }
        if profile == "postgres" {
            // PostgreSQL's C LC_CTYPE leaves every non-ASCII letter alone.
            for (predicate, expected) in [
                ("upper(name_text) = 'ǄUNGLA'", vec!["Ǆungla"]),
                ("upper(name_text) = 'İSTANBUL'", vec!["İstanbul"]),
                ("upper(name_text) = 'ID'", vec![]),
                ("lower(name_text) = 'ǆungla'", vec!["ǆungla"]),
                ("lower(name_text) = 'istanbul'", vec!["istanbul"]),
                ("lower(name_text) = 'i̇stanbul'", vec![]),
            ] {
                let (rows, _, _) = profiled(&mut session, profile, &select(predicate));
                assert_eq!(rows, expected, "{profile}: {predicate}");
            }
        }

        // The stored key does not prove a narrowing cast, which changes the
        // text `upper` sees: it may scan, but must not lose rows to the index.
        if profile == "postgres" {
            let (rows, _, _) = profiled(
                &mut session,
                profile,
                &select("upper(CAST(name_text AS VARCHAR(3))) = 'AD_'"),
            );
            assert_eq!(
                rows,
                vec!["AD_Table", "ad_table"],
                "{profile}: narrowing cast"
            );
        }

        // A transaction's own pending row is found through the same key.
        run(&mut session, profile, "BEGIN").unwrap();
        run(
            &mut session,
            profile,
            "INSERT INTO dict_names VALUES (900, 8, 'Ad_TaBlE')",
        )
        .unwrap();
        let (rows, _, _) = profiled(
            &mut session,
            profile,
            &select("upper(name_text) = 'AD_TABLE'"),
        );
        assert_eq!(
            rows,
            vec!["AD_Table", "ad_table", "Ad_TaBlE"],
            "{profile}: pending row"
        );
        run(&mut session, profile, "ROLLBACK").unwrap();
        let (rows, scans, _) = profiled(
            &mut session,
            profile,
            &select("upper(name_text) = 'AD_TABLE'"),
        );
        assert_eq!(
            rows,
            vec!["AD_Table", "ad_table"],
            "{profile}: after rollback"
        );
        assert_eq!(scans, 0, "{profile}: scanned after rollback");
    }
}
