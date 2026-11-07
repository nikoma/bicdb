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

/// A composite key whose leading column is NUMERIC/NUMBER has typed record
/// ids. A lookup by the leading column alone (the target of a by-key UPDATE,
/// or a translation row reached from its parent) used to scan every record id
/// of the table and parse every record to compare the prefix, once per
/// lookup and once per outer row under correlated subqueries (~460 ms per
/// statement on a 3k-row translation table). The canonical typed prefix is a
/// physical record-id prefix: only the matching rows are read.
#[test]
fn typed_composite_key_prefix_lookups_read_only_matching_rows() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let root = tempfile::tempdir().unwrap();
        let config = bicdb_core::DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(bicdb_core::StorageMode::ServerPaged);
        let (key, text) = if profile == "postgres" {
            ("NUMERIC", "VARCHAR")
        } else {
            ("NUMBER", "VARCHAR")
        };
        {
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
            let mut session = SqlSession::new(&mut db);
            for sql in [
                format!("CREATE TABLE col (ad_column_id {key} PRIMARY KEY, ad_element_id {key})"),
                format!(
                    "CREATE TABLE element_trl (ad_element_id {key}, ad_language {text}(6), \
                     printname {text}(60), PRIMARY KEY (ad_element_id, ad_language))"
                ),
                format!(
                    "CREATE TABLE client (ad_client_id {key} PRIMARY KEY, ismultilingualdocument CHAR(1))"
                ),
                format!(
                    "CREATE TABLE item_trl (ad_printformatitem_id {key}, ad_language {text}(6), \
                     ad_client_id {key}, printname {text}(60), \
                     PRIMARY KEY (ad_printformatitem_id, ad_language))"
                ),
                "INSERT INTO client VALUES (0, 'Y')".to_string(),
                "INSERT INTO client VALUES (11, 'N')".to_string(),
            ] {
                run(&mut session, profile, &sql).unwrap();
            }
            for c in 1..=500 {
                run(
                    &mut session,
                    profile,
                    &format!("INSERT INTO col VALUES ({c}, {c})"),
                )
                .unwrap();
                for language in ["de_DE", "es_CO"] {
                    run(
                        &mut session,
                        profile,
                        &format!(
                            "INSERT INTO element_trl VALUES ({c}, '{language}', 'E{c} {language}')"
                        ),
                    )
                    .unwrap();
                }
            }
            for item in 1..=1000 {
                for language in ["de_DE", "es_CO"] {
                    run(
                        &mut session,
                        profile,
                        &format!("INSERT INTO item_trl VALUES ({item}, '{language}', 0, 'old')"),
                    )
                    .unwrap();
                }
            }
            drop(session);
            db.close().unwrap();
        }
        // Reopen so every table is lazy, as in a long-lived database.
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
        let mut session = SqlSession::new(&mut db);
        // (rows, records fetched by id, whole-table record scans) of one
        // statement. A prefix id scan in server-paged
        // storage is a record scan that decodes every row, so key lookups
        // must report none. Each statement runs twice and the second run is
        // measured: the first statement after the reopen also loads
        // once-per-database catalog memos (the schema list behind the
        // relation-OID memo is one record scan of the schema catalog), which
        // is not part of a lookup. A per-lookup scan repeats on every run.
        let reads = |session: &mut SqlSession<'_>, sql: &str| {
            let walks_before = bicdb_core::paged_live_count_walks_on_this_thread();
            profiled(session, profile, sql);
            crate::reset_sql_record_storage_get_calls();
            let scans_before = bicdb_core::paged_record_scans_on_this_thread();
            let (rows, _) = profiled(session, profile, sql);
            let scans = bicdb_core::paged_record_scans_on_this_thread() - scans_before;
            let walks = bicdb_core::paged_live_count_walks_on_this_thread() - walks_before;
            assert_eq!(walks, 0, "{profile}: {sql} walked a table to count rows");
            (rows, crate::sql_record_storage_get_calls(), scans)
        };

        let (rows, records, scans) = reads(
            &mut session,
            "SELECT ad_language, printname FROM item_trl WHERE ad_printformatitem_id = 7 ORDER BY ad_language",
        );
        assert_eq!(
            rows,
            vec![vec!["de_DE", "old"], vec!["es_CO", "old"]],
            "{profile}"
        );
        assert!(
            records < 20,
            "{profile}: a leading-key lookup read {records} records of a 2000-row table"
        );
        assert_eq!(scans, 0, "{profile}: a leading-key lookup scanned a table");
        // Exact typed keys (one- and two-column) are located without a probe
        // that decoded the record before it was fetched.
        for (sql, expected) in [
            ("SELECT ad_element_id FROM col WHERE ad_column_id = 42", vec![vec!["42"]]),
            (
                "SELECT printname FROM element_trl WHERE ad_element_id = 42 AND ad_language = 'es_CO'",
                vec![vec!["E42 es_CO"]],
            ),
            ("SELECT ad_element_id FROM col WHERE ad_column_id = 4242", vec![]),
            (
                "SELECT printname FROM element_trl WHERE ad_element_id = 42 AND ad_language = 'fr_FR'",
                vec![],
            ),
        ] {
            let probes_before = crate::sql_primary_key_existence_probes();
            let (rows, records, scans) = reads(&mut session, sql);
            // Both runs of the statement: a canonical typed key is its own
            // locator. Probing it first decoded the whole record only to
            // drop it before the fetch decoded it again.
            let probes = crate::sql_primary_key_existence_probes() - probes_before;
            assert_eq!(probes, 0, "{profile}: {sql} probed its key with a decode");
            let expected: Vec<Vec<String>> = expected
                .into_iter()
                .map(|row: Vec<&str>| row.into_iter().map(str::to_string).collect())
                .collect();
            assert_eq!(rows, expected, "{profile}: {sql}");
            assert!(records <= 2, "{profile}: {sql} read {records} records");
            assert_eq!(scans, 0, "{profile}: {sql} scanned a table");
        }

        run(&mut session, profile, "BEGIN").unwrap();
        // The application statement: a by-key UPDATE of every language row
        // of one item, a SET subquery and an EXISTS that reach the
        // translation through a comma join, and an EXISTS on its client.
        let keyed_update = "UPDATE item_trl SET printname = (SELECT e.printname FROM element_trl e, col c \
             WHERE e.ad_language = item_trl.ad_language AND e.ad_element_id = c.ad_element_id \
             AND c.ad_column_id = 42) \
             WHERE ad_printformatitem_id = 7 \
             AND EXISTS (SELECT * FROM element_trl e, col c WHERE e.ad_language = item_trl.ad_language \
             AND e.ad_element_id = c.ad_element_id AND c.ad_column_id = 42 \
             AND item_trl.ad_printformatitem_id = 7)";
        let update = format!(
            "{keyed_update} AND EXISTS (SELECT * FROM client \
             WHERE ad_client_id = item_trl.ad_client_id AND ismultilingualdocument = 'Y')"
        );
        let (_, records, scans) = reads(&mut session, &update);
        assert!(
            records < 60,
            "{profile}: updating 2 rows by key read {records} records"
        );
        // Only the client EXISTS's statement-local candidate index reads its
        // (two-row) table whole; the key lookups scan nothing.
        assert!(
            scans <= 1,
            "{profile}: updating 2 rows by key ran {scans} table scans"
        );
        let (_, _, scans) = reads(&mut session, keyed_update);
        assert_eq!(scans, 0, "{profile}: the by-key UPDATE scanned a table");
        let (rows, _, _) = reads(
            &mut session,
            "SELECT ad_language, printname FROM item_trl WHERE ad_printformatitem_id = 7 ORDER BY ad_language",
        );
        assert_eq!(
            rows,
            vec![vec!["de_DE", "E42 de_DE"], vec!["es_CO", "E42 es_CO"]],
            "{profile}"
        );
        // Untouched neighbours keep their value; a key that matches nothing
        // and a non-integral key still find nothing.
        let (rows, _, _) = reads(
            &mut session,
            "SELECT printname FROM item_trl WHERE ad_printformatitem_id = 8 ORDER BY ad_language",
        );
        assert_eq!(rows, vec![vec!["old"], vec!["old"]], "{profile}");
        for sql in [
            "SELECT printname FROM item_trl WHERE ad_printformatitem_id = 5000",
            "SELECT printname FROM item_trl WHERE ad_printformatitem_id = 7.5",
        ] {
            let (rows, records, scans) = reads(&mut session, sql);
            assert!(rows.is_empty(), "{profile}: {sql}");
            assert!(records < 20, "{profile}: {sql} read {records} records");
            assert_eq!(scans, 0, "{profile}: {sql} scanned a table");
        }
        // Key-range candidates may include a row deleted in this transaction
        // (or an inserted one): visibility still decides.
        run(
            &mut session,
            profile,
            "DELETE FROM item_trl WHERE ad_printformatitem_id = 9 AND ad_language = 'de_DE'",
        )
        .unwrap();
        run(
            &mut session,
            profile,
            "INSERT INTO item_trl VALUES (9, 'fr_FR', 0, 'new')",
        )
        .unwrap();
        let (rows, _, scans) = reads(
            &mut session,
            "SELECT ad_language, printname FROM item_trl WHERE ad_printformatitem_id = 9 ORDER BY ad_language",
        );
        assert_eq!(
            rows,
            vec![vec!["es_CO", "old"], vec!["fr_FR", "new"]],
            "{profile}"
        );
        assert_eq!(scans, 0, "{profile}: a leading-key lookup scanned a table");
        // Equal NUMERIC spellings share the canonical prefix.
        let (rows, _, _) = reads(
            &mut session,
            "SELECT count(*) FROM item_trl WHERE ad_printformatitem_id = 7.000",
        );
        assert_eq!(rows, vec![vec!["2"]], "{profile}");
        run(&mut session, profile, "ROLLBACK").unwrap();
    }
}

/// A correlated NOT EXISTS whose correlation column is only a non-leading
/// column of a composite index (`fld(ad_tab_id, ad_column_id)`): its
/// statement-local candidate set used to come from a scan of the inner
/// table, which in server-paged storage decodes every wide row (~0.8 s on a
/// 21k-row table, for 74 outer rows). The candidates now come from the
/// index entries alone, and only the inner rows whose column occurs among
/// the outer values are read.
#[test]
fn correlated_not_exists_on_non_leading_index_column_reads_index_not_table() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let root = tempfile::tempdir().unwrap();
        let config = bicdb_core::DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(bicdb_core::StorageMode::ServerPaged);
        let key = if profile == "postgres" {
            "NUMERIC(10)"
        } else {
            "NUMBER(10)"
        };
        // Outer item `i` of format 1 (i <= 40) or 2: its column, whether it
        // is active, and its sequence (descending in `i`).
        let item_column = |i: i64| -> Option<i64> {
            match i {
                5 | 13 => None,
                39 | 40 => Some(9000 + i),
                41.. => Some((i - 40) * 3),
                _ => Some(i),
            }
        };
        // Inner field of column `c` on tab `t` is protected when encrypted
        // (c % 7 == 0 on tab 2) or obscured (c % 11 == 0 on tab 3).
        let protected = |c: i64| c % 7 == 0 || c % 11 == 0;
        {
            let mut db = BicDb::open_with_config(root.path(), config.clone()).unwrap();
            crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
            let mut session = SqlSession::new(&mut db);
            for sql in [
                format!(
                    "CREATE TABLE pfi (ad_printformatitem_id {key} PRIMARY KEY, \
                     ad_printformat_id {key}, ad_column_id {key}, isactive CHAR(1), seqno {key})"
                ),
                "CREATE INDEX pfi_format ON pfi (ad_printformat_id)".to_string(),
                format!(
                    "CREATE TABLE fld (ad_field_id {key} PRIMARY KEY, ad_tab_id {key}, \
                     ad_column_id {key}, isencrypted CHAR(1), obscuretype VARCHAR(3), \
                     description VARCHAR(400))"
                ),
                "CREATE UNIQUE INDEX fld_column ON fld (ad_tab_id, ad_column_id)".to_string(),
            ] {
                run(&mut session, profile, &sql).unwrap();
            }
            let wide = "x".repeat(300);
            for c in 1..=600i64 {
                for t in 1..=3i64 {
                    let encrypted = if t == 2 && c % 7 == 0 { "Y" } else { "N" };
                    let obscure = if t == 3 && c % 11 == 0 { "'M'" } else { "NULL" };
                    run(
                        &mut session,
                        profile,
                        &format!(
                            "INSERT INTO fld VALUES ({}, {t}, {c}, '{encrypted}', {obscure}, '{wide}')",
                            t * 1000 + c
                        ),
                    )
                    .unwrap();
                }
            }
            // Protected fields without a column: a NULL never correlates.
            for f in 1..=20i64 {
                run(
                    &mut session,
                    profile,
                    &format!(
                        "INSERT INTO fld VALUES ({}, {}, NULL, 'Y', 'M', '{wide}')",
                        5000 + f,
                        4 + f
                    ),
                )
                .unwrap();
            }
            for i in 1..=60i64 {
                let format = if i <= 40 { 1 } else { 2 };
                let column = item_column(i).map_or("NULL".to_string(), |c| c.to_string());
                let active = if i == 20 { "N" } else { "Y" };
                run(
                    &mut session,
                    profile,
                    &format!(
                        "INSERT INTO pfi VALUES ({i}, {format}, {column}, '{active}', {})",
                        100 - i
                    ),
                )
                .unwrap();
            }
            drop(session);
            db.close().unwrap();
        }
        // Reopen so every table is lazy, as in a long-lived database.
        let mut db = BicDb::open_with_config(root.path(), config).unwrap();
        crate::oracle::ensure_database_profile(&mut db, profile).unwrap();
        let mut session = SqlSession::new(&mut db);
        // (rows, records fetched by id, whole-table record scans) of the
        // second of two runs (the first also loads once-per-database memos).
        let reads = |session: &mut SqlSession<'_>, sql: &str| {
            profiled(session, profile, sql);
            crate::reset_sql_record_storage_get_calls();
            let scans_before = bicdb_core::paged_record_scans_on_this_thread();
            let gets_before = bicdb_core::paged_record_gets_on_this_thread();
            let (rows, _) = profiled(session, profile, sql);
            let scans = bicdb_core::paged_record_scans_on_this_thread() - scans_before;
            let gets = bicdb_core::paged_record_gets_on_this_thread() - gets_before;
            (
                rows,
                crate::sql_record_storage_get_calls().max(gets as usize),
                scans,
            )
        };
        let not_exists = "SELECT pfi.ad_printformatitem_id FROM pfi \
             WHERE pfi.ad_printformat_id = 1 AND pfi.isactive = 'Y' \
             AND NOT EXISTS (SELECT * FROM fld f WHERE pfi.ad_column_id = f.ad_column_id \
             AND (f.isencrypted = 'Y' OR f.obscuretype IS NOT NULL)) ORDER BY seqno";
        let application = "SELECT pfi.ad_printformatitem_id FROM pfi \
             WHERE pfi.ad_printformat_id = 1 AND pfi.isactive = 'Y' \
             AND NOT EXISTS (SELECT * FROM fld f WHERE pfi.ad_column_id = f.ad_column_id \
             AND (f.isencrypted = 'Y' OR f.obscuretype IS NOT NULL)) \
             AND ad_column_id NOT IN (SELECT pfi.ad_column_id FROM pfi \
             WHERE pfi.ad_printformat_id = 2 AND pfi.ad_column_id IS NOT NULL) ORDER BY seqno";
        let expected = |exclude_format_2: bool| -> Vec<Vec<String>> {
            let format_2 = (41..=60).filter_map(item_column).collect::<Vec<_>>();
            (1..=40i64)
                .rev()
                .filter(|&i| i != 20)
                .filter(|&i| item_column(i).is_none_or(|c| !protected(c) || c > 600))
                // NULL NOT IN a non-empty set is NULL: not selected.
                .filter(|&i| {
                    !exclude_format_2 || item_column(i).is_some_and(|c| !format_2.contains(&c))
                })
                .map(|i| vec![i.to_string()])
                .collect()
        };
        for (sql, exclude_format_2) in [(not_exists, false), (application, true)] {
            let (rows, records, scans) = reads(&mut session, sql);
            assert_eq!(rows, expected(exclude_format_2), "{profile}: {sql}");
            assert_eq!(
                scans, 0,
                "{profile}: the NOT EXISTS candidates scanned the inner table"
            );
            // 40 outer items reach at most 3 fields each.
            assert!(
                records < 400,
                "{profile}: {sql} read {records} records of a 1820-row table"
            );
        }
        // Buffered writes to the inner table keep the scanned candidates,
        // and the results follow the transaction's own writes: column 1
        // gains an encrypted field, column 14 loses its only one.
        run(&mut session, profile, "BEGIN").unwrap();
        run(
            &mut session,
            profile,
            "UPDATE fld SET isencrypted = 'Y' WHERE ad_field_id = 1001",
        )
        .unwrap();
        run(
            &mut session,
            profile,
            "UPDATE fld SET ad_column_id = 700 WHERE ad_field_id = 2014",
        )
        .unwrap();
        let (rows, _, _) = reads(&mut session, not_exists);
        let mut changed = expected(false);
        changed.retain(|row| row[0] != "1");
        changed.push(vec!["14".to_string()]);
        changed.sort_by_key(|row| std::cmp::Reverse(row[0].parse::<i64>().unwrap()));
        assert_eq!(rows, changed, "{profile}: pending writes");
        run(&mut session, profile, "ROLLBACK").unwrap();
        let (rows, _, scans) = reads(&mut session, not_exists);
        assert_eq!(rows, expected(false), "{profile}: after rollback");
        assert_eq!(scans, 0, "{profile}: after rollback");
    }
}
