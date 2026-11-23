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
