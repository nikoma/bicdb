//! Inner joins that reach every relation by primary key from one keyed row
//! (a reference lookup joined to its table and two of its columns) must use
//! key lookups only, never a scan of the joined tables.
use super::*;

fn run(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> Result<SqlResult> {
    if profile == "postgres" {
        return session.execute(sql);
    }
    let sql = crate::oracle::lower_session_sql(session.db_ref(), sql, profile, None, "bicdb")?;
    session.execute(&sql)
}

fn profiled(session: &mut SqlSession<'_>, profile: &str, sql: &str) -> (Vec<Vec<String>>, usize) {
    crate::SQL_PROFILE_STACK
        .with(|stack| stack.borrow_mut().push(crate::SqlProfileStats::default()));
    let result = run(session, profile, sql);
    let stats = crate::SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    let rows = result
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|value| value.to_cell()).collect())
        .collect();
    (rows, stats.full_scan_count)
}

#[test]
fn keyed_reference_lookup_joins_use_primary_keys_only() {
    for (profile, mode) in [
        ("postgres", bicdb_core::StorageMode::EmbeddedMemory),
        ("oracle-preview-v1", bicdb_core::StorageMode::EmbeddedMemory),
        ("postgres", bicdb_core::StorageMode::ServerPaged),
        ("oracle-preview-v1", bicdb_core::StorageMode::ServerPaged),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode.clone()),
        )
        .unwrap();
        crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
        let mut session = SqlSession::new(&mut db);
        // Application keys are exact NUMERIC/NUMBER(10), not INTEGER.
        let key = if profile == "postgres" {
            "NUMERIC(10)"
        } else {
            "NUMBER(10)"
        };
        for sql in [
            format!("CREATE TABLE ref_table (ad_reference_id {key} PRIMARY KEY, ad_table_id {key}, ad_key {key}, ad_display {key}, isactive CHAR(1))"),
            format!("CREATE TABLE tab (ad_table_id {key} PRIMARY KEY, tablename VARCHAR(40), isactive CHAR(1))"),
            format!("CREATE TABLE col (ad_column_id {key} PRIMARY KEY, columnname VARCHAR(40), istranslated CHAR(1))"),
        ] {
            run(&mut session, profile, &sql).unwrap();
        }
        for t in 1..=50 {
            run(
                &mut session,
                profile,
                &format!("INSERT INTO tab VALUES ({t}, 'T{t}', 'Y')"),
            )
            .unwrap();
        }
        for c in 1..=2000 {
            run(
                &mut session,
                profile,
                &format!("INSERT INTO col VALUES ({c}, 'C{c}', 'N')"),
            )
            .unwrap();
        }
        for r in 1..=100 {
            let table = r % 50 + 1;
            run(
                &mut session,
                profile,
                &format!(
                    "INSERT INTO ref_table VALUES ({r}, {table}, {}, {}, 'Y')",
                    r * 10,
                    r * 10 + 1
                ),
            )
            .unwrap();
        }
        let query = |reference: i64| {
            format!(
                "SELECT t.tablename, ck.columnname AS keycolumn, cd.columnname AS displaycolumn, cd.istranslated \
                 FROM ref_table rt INNER JOIN tab t ON (rt.ad_table_id = t.ad_table_id) \
                 INNER JOIN col ck ON (rt.ad_key = ck.ad_column_id) \
                 INNER JOIN col cd ON (rt.ad_display = cd.ad_column_id) \
                 WHERE rt.ad_reference_id = {reference} AND rt.isactive = 'Y' AND t.isactive = 'Y'"
            )
        };
        let (rows, scans) = profiled(&mut session, profile, &query(7));
        assert_eq!(rows, vec![vec!["T8", "C70", "C71", "N"]], "{profile}");
        assert_eq!(scans, 0, "{profile}: key-to-key joins scanned a table");
        let (rows, scans) = profiled(
            &mut session,
            profile,
            "SELECT ck.columnname FROM col ck WHERE ck.ad_column_id = (SELECT ad_key FROM ref_table WHERE ad_reference_id = 7)",
        );
        assert_eq!(rows, vec![vec!["C70"]], "{profile}");
        assert_eq!(scans, 0, "{profile}: a key from a scalar subquery scanned");
        // A subquery that finds no row binds the key to NULL: `pk = NULL` is
        // never TRUE, so nothing is read (this used to scan all of `col`).
        let (rows, scans) = profiled(
            &mut session,
            profile,
            "SELECT ck.columnname FROM col ck WHERE ck.ad_column_id = (SELECT ad_key FROM ref_table WHERE ad_reference_id = 9999)",
        );
        assert!(rows.is_empty(), "{profile}");
        assert_eq!(
            scans, 0,
            "{profile}: a NULL key from a scalar subquery scanned"
        );
        let (rows, scans) = profiled(&mut session, profile, &query(9999));
        assert!(rows.is_empty(), "{profile}");
        assert_eq!(scans, 0, "{profile}: a missing reference scanned a table");
        drop(session);

        // Another transaction's uncommitted writes to the joined tables must
        // not turn the key lookups into scans.
        let mut writer = SqlSession::new_shared(&db);
        let mut reader = SqlSession::new_shared(&db);
        run(&mut writer, profile, "BEGIN").unwrap();
        run(
            &mut writer,
            profile,
            "UPDATE col SET istranslated = 'Y' WHERE ad_column_id <= 500",
        )
        .unwrap();
        run(
            &mut writer,
            profile,
            "UPDATE tab SET tablename = tablename WHERE ad_table_id <= 10",
        )
        .unwrap();
        let started = std::time::Instant::now();
        let (rows, scans) = profiled(&mut reader, profile, &query(7));
        let elapsed = started.elapsed();
        assert_eq!(rows, vec![vec!["T8", "C70", "C71", "N"]], "{profile}");
        assert_eq!(
            scans, 0,
            "{profile}: pending writes elsewhere made the lookup scan ({elapsed:?})"
        );
        run(&mut writer, profile, "ROLLBACK").unwrap();
    }
}

/// After a reopen every server-paged table is lazy (no resident rows), and
/// an exact row count of one is a visibility-checked walk of every row. Join
/// ordering must take the cheap planner estimate instead: the join-order memo
/// is keyed by the statement's literals, so each new key value used to walk
/// the whole joined table before its single key lookup (~250 ms per value on
/// a 26k-row wide table).
#[test]
fn key_joins_on_reopened_paged_tables_do_not_count_rows_per_plan() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let root = tempfile::tempdir().unwrap();
        let config = bicdb_core::DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(bicdb_core::StorageMode::ServerPaged);
        let key = if profile == "postgres" {
            "NUMERIC"
        } else {
            "NUMBER"
        };
        {
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
            let mut session = SqlSession::new(&mut db);
            for sql in [
                format!("CREATE TABLE ref_table (ad_reference_id {key} PRIMARY KEY, ad_key {key})"),
                format!(
                    "CREATE TABLE col (ad_column_id {key} PRIMARY KEY, columnname VARCHAR(40))"
                ),
            ] {
                run(&mut session, profile, &sql).unwrap();
            }
            for c in 1..=2000 {
                run(
                    &mut session,
                    profile,
                    &format!("INSERT INTO col VALUES ({c}, 'C{c}')"),
                )
                .unwrap();
            }
            for r in 1..=20 {
                run(
                    &mut session,
                    profile,
                    &format!("INSERT INTO ref_table VALUES ({r}, {})", r * 10),
                )
                .unwrap();
            }
            drop(session);
            db.close().unwrap();
        }
        // Reopen: the collections are now lazy, the state of every
        // long-lived server-paged database.
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
        let mut session = SqlSession::new(&mut db);
        for reference in 1..=5i64 {
            let sql = format!(
                "SELECT ck.columnname FROM ref_table rt INNER JOIN col ck \
                 ON (rt.ad_key = ck.ad_column_id) WHERE rt.ad_reference_id = {reference}"
            );
            let walks_before = bicdb_core::paged_live_count_walks_on_this_thread();
            let (rows, scans) = profiled(&mut session, profile, &sql);
            let walks = bicdb_core::paged_live_count_walks_on_this_thread() - walks_before;
            assert_eq!(
                rows,
                vec![vec![format!("C{}", reference * 10)]],
                "{profile}"
            );
            assert_eq!(scans, 0, "{profile}: key-to-key join scanned a table");
            assert_eq!(
                walks, 0,
                "{profile}: planning reference {reference} walked a table to count its rows"
            );
        }
    }
}
