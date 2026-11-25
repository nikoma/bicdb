//! Regressions found by running the Frappe Framework (PostgreSQL mode) against
//! BicDB. Each test states the PostgreSQL behaviour the framework relies on.
use bicdb_core::BicDb;
use bicdb_sql::{SqlEngine, SqlSession, SqlValue};
use std::{collections::HashMap, sync::Arc};

fn s(value: &str) -> SqlValue {
    SqlValue::String(value.to_string())
}

fn gucs(database: &str) -> HashMap<String, String> {
    HashMap::from([("database".to_string(), database.to_string())])
}

/// Frappe lists its tables with
/// `information_schema.tables WHERE table_catalog = <db name>`; PostgreSQL
/// reports the connected database as every information_schema catalog name.
#[test]
fn information_schema_catalog_is_the_connected_database() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut session = SqlSession::new(&mut db).with_session_gucs(gucs("site_db"));
        session
            .execute(
                "CREATE TABLE \"tabDefaultValue\" (name varchar(140) PRIMARY KEY, defkey text UNIQUE); \
                 CREATE VIEW v_defaults AS SELECT name FROM \"tabDefaultValue\"",
            )
            .unwrap();
        let tables = session
            .execute(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_catalog = 'site_db' AND table_type = 'BASE TABLE' AND table_schema = 'public'",
            )
            .unwrap();
        assert_eq!(tables.rows, vec![vec![s("tabDefaultValue")]]);
        for query in [
            "SELECT DISTINCT table_catalog FROM information_schema.columns WHERE table_name = 'tabDefaultValue'",
            "SELECT DISTINCT table_catalog FROM information_schema.views",
            "SELECT DISTINCT constraint_catalog FROM information_schema.table_constraints WHERE table_name = 'tabDefaultValue'",
            "SELECT current_database()",
        ] {
            assert_eq!(session.execute(query).unwrap().rows, vec![vec![s("site_db")]], "{query}");
        }
    }
    let engine = SqlEngine::new(&db).with_session_gucs(Arc::new(gucs("other_db")));
    assert_eq!(
        engine
            .execute("SELECT DISTINCT table_catalog FROM information_schema.tables WHERE table_name = 'tabDefaultValue'")
            .unwrap()
            .rows,
        vec![vec![s("other_db")]]
    );
    // Without a connected database (embedded use) the historical name stays.
    let engine = SqlEngine::new(&db);
    assert_eq!(
        engine
            .execute("SELECT DISTINCT table_catalog FROM information_schema.tables WHERE table_name = 'tabDefaultValue'")
            .unwrap()
            .rows,
        vec![vec![s("bicdb")]]
    );
}

/// `substring(text, pattern)` with a text pattern is the regular-expression
/// form (Frappe matches index definitions with `SUBSTRING(indexdef, '(.*)')`).
#[test]
fn substring_with_a_text_pattern_is_a_regex_match() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE defs (id int PRIMARY KEY, def text); INSERT INTO defs VALUES (1, 'CREATE INDEX i ON t USING btree (parent)')")
        .unwrap();
    for (query, expected) in [
        (
            "SELECT substring('CREATE INDEX i ON t (parent)', '\\((.*)\\)')",
            s("parent"),
        ),
        ("SELECT substring('Thomas' FROM '...$')", s("mas")),
        ("SELECT substring('Thomas', 'zz')", SqlValue::Null),
        ("SELECT substring('Thomas', 2)", s("homas")),
        ("SELECT substring('Thomas' FROM 2 FOR 3)", s("hom")),
        (
            "SELECT SUBSTRING(def, '(.*)') FROM defs",
            s("CREATE INDEX i ON t USING btree (parent)"),
        ),
        (
            "SELECT SUBSTRING(def, 'btree \\((\\w+)\\)') FROM defs WHERE id = 1",
            s("parent"),
        ),
    ] {
        assert_eq!(
            session.execute(query).unwrap().rows,
            vec![vec![expected]],
            "{query}"
        );
    }
}

fn rows(session: &mut SqlSession<'_>, query: &str) -> Vec<Vec<SqlValue>> {
    session.execute(query).unwrap().rows
}

/// Frappe syncs a DocType's table inside the transaction that saved the
/// DocType row, so ALTER TABLE must see (and not conflict with) rows the same
/// transaction already inserted, updated, locked or deleted.
#[test]
fn row_rewriting_alter_table_includes_the_transactions_own_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE doc (name varchar(140) PRIMARY KEY, a varchar(140), b int); \
             INSERT INTO doc VALUES ('1', 'one', NULL), ('2', 'two', 2), ('3', 'three', 3), ('4', 'four', 4)",
        )
        .unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO doc VALUES ('5', 'five', NULL)")
        .unwrap();
    session
        .execute("UPDATE doc SET a = 'uno' WHERE name = '1'")
        .unwrap();
    session.execute("DELETE FROM doc WHERE name = '4'").unwrap();
    session
        .execute("SELECT name FROM doc WHERE name = '3' FOR UPDATE")
        .unwrap();
    session
        .execute(
            "ALTER TABLE doc ADD COLUMN c smallint NOT NULL DEFAULT 7, \
             ALTER COLUMN a TYPE varchar(64), ALTER COLUMN b TYPE bigint",
        )
        .unwrap();
    // Frappe fills NULLs with the default, then tightens the column.
    session
        .execute("UPDATE doc SET b = 0 WHERE b IS NULL")
        .unwrap();
    session
        .execute("ALTER TABLE doc ALTER COLUMN b SET NOT NULL, ALTER COLUMN b SET DEFAULT 0")
        .unwrap();
    session.execute("ALTER TABLE doc DROP COLUMN a").unwrap();
    session.execute("COMMIT").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT name, b, c FROM doc ORDER BY name"),
        vec![
            vec![s("1"), SqlValue::Int(0), SqlValue::Int(7)],
            vec![s("2"), SqlValue::Int(2), SqlValue::Int(7)],
            vec![s("3"), SqlValue::Int(3), SqlValue::Int(7)],
            vec![s("5"), SqlValue::Int(0), SqlValue::Int(7)],
        ]
    );

    // SET NOT NULL still refuses NULLs the transaction itself wrote.
    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TABLE doc ALTER COLUMN c DROP NOT NULL")
        .unwrap();
    session
        .execute("UPDATE doc SET c = NULL WHERE name = '2'")
        .unwrap();
    assert!(session
        .execute("ALTER TABLE doc ALTER COLUMN c SET NOT NULL")
        .is_err());
    session.execute("ROLLBACK").unwrap();

    // A rolled-back ALTER restores the shape and the transaction's rows vanish.
    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO doc VALUES ('6', 6, 6)")
        .unwrap();
    session
        .execute("UPDATE doc SET b = 20 WHERE name = '2'")
        .unwrap();
    session
        .execute("ALTER TABLE doc ADD COLUMN d int NOT NULL DEFAULT 1")
        .unwrap();
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT name, b, c FROM doc ORDER BY name"),
        vec![
            vec![s("1"), SqlValue::Int(0), SqlValue::Int(7)],
            vec![s("2"), SqlValue::Int(2), SqlValue::Int(7)],
            vec![s("3"), SqlValue::Int(3), SqlValue::Int(7)],
            vec![s("5"), SqlValue::Int(0), SqlValue::Int(7)],
        ]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT count(*) FROM information_schema.columns WHERE table_name = 'doc' AND column_name = 'd'"
        ),
        vec![vec![SqlValue::Int(0)]]
    );
}

/// Frappe names single-column indexes after the column
/// (`CREATE INDEX IF NOT EXISTS "standard" ON "tabPrint Format"("standard")`).
/// Index names are unique per schema in PostgreSQL, so the second table's
/// IF NOT EXISTS is a no-op instead of an error.
#[test]
fn create_index_if_not_exists_skips_a_name_used_on_another_table() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE \"tabReport\" (name varchar(140) PRIMARY KEY, standard varchar(3)); \
             CREATE TABLE \"tabPrint Format\" (name varchar(140) PRIMARY KEY, standard varchar(3)); \
             CREATE INDEX IF NOT EXISTS \"standard\" ON \"tabReport\"(\"standard\")",
        )
        .unwrap();
    session
        .execute("CREATE INDEX IF NOT EXISTS \"standard\" ON \"tabPrint Format\"(\"standard\")")
        .unwrap();
    assert!(session
        .execute("CREATE INDEX \"standard\" ON \"tabPrint Format\"(\"standard\")")
        .is_err());
    assert_eq!(
        rows(
            &mut session,
            "SELECT tablename FROM pg_indexes WHERE indexname = 'standard'"
        ),
        vec![vec![s("tabReport")]]
    );
}

/// Another connection's open transaction (one that never touched the table)
/// must not make DROP TABLE fail, and must not leave a recycle-bin intent that
/// stops the database from reopening. Frappe's tests drop scratch tables while
/// a second connection is idle in a transaction.
#[test]
fn drop_table_with_another_transaction_pending_drops_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        SqlSession::new(&mut db)
            .execute(
                "CREATE TABLE other (id int PRIMARY KEY); INSERT INTO other VALUES (1); \
                 CREATE TABLE \"tabTestNotes\" (id int PRIMARY KEY, content text); \
                 CREATE INDEX test_index ON \"tabTestNotes\" (id, content); \
                 CREATE TABLE scratch (id int PRIMARY KEY); CREATE INDEX scratch_idx ON scratch (id); \
                 CREATE TABLE written (id int PRIMARY KEY); INSERT INTO written VALUES (1)",
            )
            .unwrap();
        let mut idle = db.begin_transaction().unwrap();
        idle.get("other", "1").unwrap();
        idle.insert("other", bicdb_core::Record::new("2")).unwrap();
        {
            let mut session = SqlSession::new(&mut db);
            session.execute("DROP TABLE \"tabTestNotes\"").unwrap();
            session.execute("BEGIN").unwrap();
            session.execute("DROP TABLE scratch").unwrap();
            session.execute("COMMIT").unwrap();
            // A table this transaction wrote cannot be dropped permanently
            // under its buffered rows: the conflict is reported, and nothing
            // is left behind for recovery.
            session.execute("BEGIN").unwrap();
            session.execute("INSERT INTO written VALUES (2)").unwrap();
            let error = session.execute("DROP TABLE written").unwrap_err();
            assert_eq!(error.sqlstate(), "40001");
            session.execute("ROLLBACK").unwrap();
            assert_eq!(
                rows(&mut session, "SELECT id FROM written"),
                vec![vec![SqlValue::Int(1)]]
            );
            session
                .execute("CREATE TABLE \"tabTestNotes\" (id int PRIMARY KEY, content text); CREATE INDEX test_index ON \"tabTestNotes\" (id)")
                .unwrap();
        }
        idle.rollback().unwrap();
    }
    let mut db = BicDb::open(dir.path()).unwrap();
    bicdb_sql::recover_interrupted_recycle_bin_renames(&mut db).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(
            &mut session,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY 1"
        ),
        vec![vec![s("other")], vec![s("tabTestNotes")], vec![s("written")]]
    );
}

/// Frappe's link checks run `WHERE fieldtype IN ('Link', 'Table') AND
/// options = ...` against tabDocField once per DocType; the IN list on the
/// indexed column must use the index (one lookup per value), and give the
/// same rows as a scan, including the transaction's own pending rows.
#[test]
fn in_list_on_an_indexed_column_uses_the_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute(
            "CREATE TABLE \"tabDocField\" (name varchar(140) PRIMARY KEY, parent varchar(140), \
             fieldtype varchar(140), options text, creation timestamp(6)); \
             CREATE INDEX ON \"tabDocField\" (fieldtype); \
             INSERT INTO \"tabDocField\" SELECT 'f' || g, 'P' || (g % 7), \
             (ARRAY['Data','Link','Table','Int','Check'])[1 + g % 5], \
             CASE WHEN g % 3 = 0 THEN 'User' ELSE 'Role' END, \
             timestamp '2026-01-01' + g * interval '1 minute' FROM generate_series(1, 300) g",
        )
        .unwrap();
    let query = "SELECT \"parent\", \"name\" FROM \"tabDocField\" WHERE \"fieldtype\" IN ('Link','Table') AND \"options\"='User' ORDER BY \"creation\" DESC";
    let plan = rows(&mut session, &format!("EXPLAIN {query}"));
    assert!(
        format!("{plan:?}").contains("InListKeys 2"),
        "expected an IN-list index lookup, got {plan:?}"
    );
    let scanned = rows(
        &mut session,
        "SELECT \"parent\", \"name\" FROM \"tabDocField\" WHERE (\"fieldtype\" = 'Link' OR \"fieldtype\" = 'Table') AND \"options\"='User' ORDER BY \"creation\" DESC",
    );
    assert_eq!(scanned.len(), 40);
    assert_eq!(rows(&mut session, query), scanned);

    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO \"tabDocField\" VALUES ('new', 'P9', 'Link', 'User', '2027-01-01')")
        .unwrap();
    session
        .execute("UPDATE \"tabDocField\" SET fieldtype = 'Data' WHERE name = 'f6'")
        .unwrap();
    let in_tx = rows(&mut session, query);
    assert_eq!(in_tx.len(), 40);
    assert_eq!(in_tx[0], vec![s("P9"), s("new")]);
    assert!(!in_tx.iter().any(|row| row[1] == s("f6")));
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT count(*) FROM \"tabDocField\" WHERE fieldtype IN ('Link', NULL, 'Link')"
        ),
        vec![vec![SqlValue::Int(60)]]
    );
}

/// `''` is an ordinary text key in PostgreSQL. Frappe's naming series
/// store it in `tabSeries` when a name template expands to an empty series
/// key (its test-record generator does), after probing it with
/// `SELECT "current" FROM "tabSeries" WHERE "name"='' FOR UPDATE`.
#[test]
fn empty_text_primary_keys_are_ordinary_keys() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(dir.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session
            .execute(
                "CREATE TABLE \"tabSeries\" (name varchar(100) PRIMARY KEY, current int); \
                 INSERT INTO \"tabSeries\" VALUES ('A', 1)",
            )
            .unwrap();
        session.execute("BEGIN").unwrap();
        assert!(rows(
            &mut session,
            "SELECT \"current\" FROM \"tabSeries\" WHERE \"name\"='' FOR UPDATE"
        )
        .is_empty());
        session
            .execute("INSERT INTO \"tabSeries\" (\"name\", \"current\") VALUES ('', 1)")
            .unwrap();
        session
            .execute("UPDATE \"tabSeries\" SET \"current\" = \"current\" + 1 WHERE \"name\"=''")
            .unwrap();
        session.execute("COMMIT").unwrap();
        for query in [
            "SELECT name, current FROM \"tabSeries\" WHERE name = ''",
            "SELECT name, current FROM \"tabSeries\" WHERE name IN ('')",
            "SELECT name, current FROM \"tabSeries\" WHERE name = ANY(ARRAY[''])",
            "SELECT name, current FROM \"tabSeries\" WHERE current = 2",
        ] {
            assert_eq!(
                rows(&mut session, query),
                vec![vec![s(""), SqlValue::Int(2)]],
                "{query}"
            );
        }
        assert_eq!(
            rows(&mut session, "SELECT name FROM \"tabSeries\" ORDER BY name"),
            vec![vec![s("")], vec![s("A")]]
        );
        assert!(session
            .execute("INSERT INTO \"tabSeries\" VALUES ('', 9)")
            .is_err());
        session
            .execute("INSERT INTO \"tabSeries\" VALUES ('', 9) ON CONFLICT (name) DO UPDATE SET current = excluded.current")
            .unwrap();
        session
            .execute(
                "CREATE TABLE child (id int PRIMARY KEY, series varchar(100) REFERENCES \"tabSeries\"(name)); \
                 INSERT INTO child VALUES (1, ''); \
                 CREATE TABLE pairs (a text, b text, PRIMARY KEY (a, b)); \
                 INSERT INTO pairs VALUES ('', ''), ('', 'x')",
            )
            .unwrap();
        assert_eq!(
            rows(
                &mut session,
                "SELECT a, b FROM pairs WHERE a = '' ORDER BY b"
            ),
            vec![vec![s(""), s("")], vec![s(""), s("x")]]
        );
        assert!(session
            .execute("DELETE FROM \"tabSeries\" WHERE name = ''")
            .is_err());
    }
    // The key survives a restart (WAL replay / checkpoint).
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    assert_eq!(
        rows(
            &mut session,
            "SELECT s.name, s.current, c.id FROM \"tabSeries\" s JOIN child c ON c.series = s.name"
        ),
        vec![vec![s(""), SqlValue::Int(9), SqlValue::Int(1)]]
    );
    session.execute("DELETE FROM child").unwrap();
    session
        .execute("DELETE FROM \"tabSeries\" WHERE name = ''")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT name FROM \"tabSeries\""),
        vec![vec![s("A")]]
    );
}

/// `date(x)` is the function spelling of `CAST(x AS date)`; Frappe's report
/// builder groups by `date(creation)`.
#[test]
fn date_function_casts_to_date() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE \"tabDocType\" (name varchar(140) PRIMARY KEY, issingle smallint, creation timestamp(6)); INSERT INTO \"tabDocType\" VALUES ('A', 0, '2026-11-23 10:11:12.5'), ('B', 1, NULL)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "select \"name\", \"issingle\", date(creation) as creation from \"tabDocType\" order by \"tabDocType\".\"creation\" DESC limit 2 offset 0"
        ),
        vec![
            vec![s("B"), SqlValue::Int(1), SqlValue::Null],
            vec![s("A"), SqlValue::Int(0), s("2026-11-23")],
        ]
    );
    assert_eq!(
        rows(&mut session, "SELECT date('2026-02-03 04:05:06'), date(timestamp '2026-02-03 23:59:59') = date '2026-02-03', pg_typeof(date(now()))"),
        vec![vec![s("2026-02-03"), SqlValue::Bool(true), s("date")]]
    );
}

/// `frappe.db.estimate_count()` reads `pg_class.reltuples`, which
/// PostgreSQL's autovacuum keeps near the row count; an unanalyzed BicDB
/// table reported 0.
#[test]
fn reltuples_estimates_rows_of_an_unanalyzed_table() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db);
    session
        .execute("CREATE TABLE \"tabDocField\" (name varchar(140) PRIMARY KEY); INSERT INTO \"tabDocField\" SELECT 'f' || g FROM generate_series(1, 150) g")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "select c.reltuples from pg_class c join pg_namespace n on n.oid = c.relnamespace \
             where c.relname = 'tabDocField' and n.nspname = 'public' and c.relkind = 'r'"
        ),
        vec![vec![SqlValue::Float(150.0)]]
    );
}

/// `frappe.db.get_database_size()`:
/// `SELECT (pg_database_size(<name>) / 1024 / 1024) AS database_size`.
#[test]
fn pg_database_size_reports_the_connected_database() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut session = SqlSession::new(&mut db).with_session_gucs(gucs("site_db"));
    session
        .execute("CREATE TABLE filler (id int PRIMARY KEY, body text); INSERT INTO filler SELECT g, repeat('x', 100) FROM generate_series(1, 200) g")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT pg_database_size('site_db') > 0"),
        vec![vec![SqlValue::Bool(true)]]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT pg_database_size(current_database()) > 0, (pg_database_size('site_db') / 1024 / 1024) >= 0 AS database_size"
        ),
        vec![vec![SqlValue::Bool(true), SqlValue::Bool(true)]]
    );
    let error = session
        .execute("SELECT pg_database_size('missing_db')")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "3D000");
}
