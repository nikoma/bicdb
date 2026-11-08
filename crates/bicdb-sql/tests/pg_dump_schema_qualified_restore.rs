//! Regression: pg_dump restores use schema-qualified names everywhere.
//! Each case below was a hard failure restoring a production dump.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn session_db() -> (tempfile::TempDir, BicDb) {
    let dir = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    (dir, db)
}

fn int_at(sql: &mut SqlSession, query: &str) -> i64 {
    match sql.execute(query).unwrap().rows[0][0] {
        SqlValue::Int(value) => value,
        ref other => panic!("expected int from {query}, got {other:?}"),
    }
}

/// pg_dump's SERIAL shape: the sequence is created separately, then bound
/// as a column default via `nextval('<schema>.<seq>'::regclass)`. The
/// schema-qualified name must resolve to the stored sequence.
#[test]
fn set_default_nextval_resolves_a_schema_qualified_sequence() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);

    sql.execute("CREATE TABLE public.serial_demo (id BIGINT NOT NULL, body TEXT)")
        .unwrap();
    sql.execute("CREATE SEQUENCE public.serial_demo_id_seq")
        .unwrap();
    sql.execute(
        "ALTER TABLE public.serial_demo ALTER COLUMN id \
         SET DEFAULT nextval('public.serial_demo_id_seq'::regclass)",
    )
    .expect("schema-qualified nextval default must resolve");

    // The default fires: inserts omitting the column get sequence values.
    sql.execute("INSERT INTO public.serial_demo (body) VALUES ('a')")
        .unwrap();
    sql.execute("INSERT INTO public.serial_demo (body) VALUES ('b')")
        .unwrap();
    let rows = sql
        .execute("SELECT id FROM public.serial_demo ORDER BY id")
        .unwrap();
    let ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|row| match row[0] {
            SqlValue::Int(v) => v,
            ref other => panic!("expected int id, got {other:?}"),
        })
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids[1] > ids[0], "sequence values must ascend: {ids:?}");

    // pg_dump then setval()s the sequence past the restored data; the next
    // insert must continue from there (same normalized key).
    sql.execute("SELECT setval('public.serial_demo_id_seq', 500, true)")
        .unwrap();
    sql.execute("INSERT INTO public.serial_demo (body) VALUES ('c')")
        .unwrap();
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT id FROM public.serial_demo WHERE body = 'c'"
        ),
        501,
        "setval on the schema-qualified name must affect the same sequence"
    );

    // A non-public schema behaves identically.
    sql.execute("CREATE SCHEMA app").unwrap();
    sql.execute("CREATE TABLE app.items (id BIGINT NOT NULL, label TEXT)")
        .unwrap();
    sql.execute("CREATE SEQUENCE app.items_id_seq").unwrap();
    sql.execute(
        "ALTER TABLE app.items ALTER COLUMN id \
         SET DEFAULT nextval('app.items_id_seq'::regclass)",
    )
    .expect("non-public schema-qualified nextval default must resolve");
    sql.execute("INSERT INTO app.items (label) VALUES ('x')")
        .unwrap();
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM app.items"), 1);

    // An genuinely missing sequence must still fail closed.
    assert!(
        sql.execute(
            "ALTER TABLE public.serial_demo ALTER COLUMN id \
             SET DEFAULT nextval('public.no_such_sequence_here'::regclass)"
        )
        .is_err(),
        "a missing sequence must still be rejected"
    );
}

/// CREATE TABLE with an inline schema-qualified nextval default (the other
/// spelling pg_dump emits) binds to the same sequence.
#[test]
fn create_table_inline_nextval_default_resolves_schema_qualified() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);

    sql.execute("CREATE SEQUENCE public.inline_seq").unwrap();
    sql.execute(
        "CREATE TABLE public.inline_demo (\
           id BIGINT NOT NULL DEFAULT nextval('public.inline_seq'::regclass), \
           body TEXT)",
    )
    .unwrap();
    sql.execute("INSERT INTO public.inline_demo (body) VALUES ('a')")
        .unwrap();
    assert_eq!(
        int_at(&mut sql, "SELECT count(*) FROM public.inline_demo"),
        1
    );
    // Bound to the same sequence object, not a fresh implicit one.
    sql.execute("SELECT setval('public.inline_seq', 900, true)")
        .unwrap();
    sql.execute("INSERT INTO public.inline_demo (body) VALUES ('b')")
        .unwrap();
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT id FROM public.inline_demo WHERE body = 'b'"
        ),
        901
    );
}

/// Regression: a pg_dump restore creates a trigger function in a
/// non-public schema and then a trigger that calls it schema-qualified.
/// Routines key on the bare name everywhere, but the AST CREATE FUNCTION
/// path used to hash the schema into the key, so the trigger's lookup
/// (raw path, bare name) never found it — "function does not exist".
#[test]
fn schema_qualified_trigger_function_resolves_and_is_visible_in_pg_proc() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);

    sql.execute("CREATE SCHEMA carrier_private").unwrap();
    sql.execute("CREATE TABLE public.carrier_events (id TEXT PRIMARY KEY, body TEXT)")
        .unwrap();
    sql.execute(
        r#"CREATE FUNCTION carrier_private.enforce_operation_context()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, carrier_private
AS $$ BEGIN RETURN NEW; END; $$"#,
    )
    .unwrap();

    // The trigger must find the function through its schema-qualified name.
    sql.execute(
        "CREATE TRIGGER enforce_ctx BEFORE INSERT ON public.carrier_events \
         FOR EACH ROW EXECUTE FUNCTION carrier_private.enforce_operation_context()",
    )
    .expect("trigger must resolve a schema-qualified function");

    // And pg_proc must report it under its real schema, not public.
    let rows = sql
        .execute(
            "SELECT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE p.proname = 'enforce_operation_context'",
        )
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::String("carrier_private".to_string())]],
        "the function must appear in pg_proc under carrier_private"
    );

    // Writes through the triggered table still work.
    sql.execute("INSERT INTO public.carrier_events VALUES ('e1', 'x')")
        .unwrap();
    assert_eq!(
        int_at(&mut sql, "SELECT count(*) FROM public.carrier_events"),
        1
    );

    // A public-schema function still reports public.
    sql.execute(
        "CREATE FUNCTION public.plain_fn() RETURNS trigger \
         LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$",
    )
    .unwrap();
    let rows = sql
        .execute(
            "SELECT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE p.proname = 'plain_fn'",
        )
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::String("public".to_string())]]
    );
}

/// A dump restores into its own schema; the application then names its
/// relations unqualified and resolves them through `search_path`, including
/// inside PL/pgSQL bodies, which resolve with the caller's path.
#[test]
fn unqualified_relations_resolve_through_search_path() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE SCHEMA app").unwrap();
    sql.execute("CREATE TABLE app.item (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    sql.execute("INSERT INTO app.item VALUES (1, 'one')")
        .unwrap();
    sql.execute("CREATE TABLE public.only_public (id INT)")
        .unwrap();
    sql.execute(
        "CREATE FUNCTION app.item_count() RETURNS bigint LANGUAGE plpgsql AS $$ \
         DECLARE n bigint; BEGIN SELECT count(*) INTO n FROM item; RETURN n; END; $$",
    )
    .unwrap();

    // Default path ("$user", public) has no schema for this user.
    assert!(sql.execute("SELECT count(*) FROM item").is_err());

    sql.execute("SET search_path TO app, public").unwrap();
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM item"), 1);
    assert_eq!(
        int_at(&mut sql, "SELECT count(*) FROM item WHERE item.id = 1"),
        1
    );
    // Unquoted mixed-case names fold to lower case, with or without a
    // predicate, and weak row-lock strengths bind too.
    assert_eq!(
        sql.execute("SELECT Name FROM Item").unwrap().rows,
        vec![vec![SqlValue::String("one".into())]]
    );
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT ID FROM Item WHERE ID = 1 FOR NO KEY UPDATE"
        ),
        1
    );
    sql.execute("CREATE UNIQUE INDEX item_name_key ON Item (COALESCE(Name, ''), COALESCE(ID, 0))")
        .expect("unquoted mixed-case names fold in index expressions");
    sql.execute("INSERT INTO item VALUES (2, 'two')").unwrap();
    sql.execute("UPDATE item SET name = 'deux' WHERE id = 2")
        .unwrap();
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM app.item"), 2);
    // A name the first schema lacks falls through to the next one.
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM only_public"), 0);
    // A CTE shadows a relation of the same name.
    assert_eq!(
        int_at(
            &mut sql,
            "WITH item AS (SELECT 5 AS id) SELECT id FROM item"
        ),
        5
    );
    // New relations go to the first schema on the path, and references
    // (including foreign keys) bind to it.
    sql.execute("CREATE TABLE line (id INT PRIMARY KEY, item_id INT REFERENCES item (id))")
        .unwrap();
    sql.execute("INSERT INTO line VALUES (10, 1)").unwrap();
    assert!(sql.execute("INSERT INTO line VALUES (11, 99)").is_err());
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM app.line"), 1);
    assert!(sql.execute("SELECT count(*) FROM public.line").is_err());
    sql.execute("CREATE VIEW item_names AS SELECT name FROM item")
        .unwrap();
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM app.item_names"), 2);
    // The body resolves with the caller's path.
    assert_eq!(int_at(&mut sql, "SELECT app.item_count()"), 2);
    sql.execute("DELETE FROM item WHERE id = 2").unwrap();
    sql.execute("DROP VIEW item_names").unwrap();
    assert!(sql.execute("SELECT count(*) FROM app.item_names").is_err());

    // "$user" expands to the current role.
    sql.execute("CREATE ROLE app LOGIN SUPERUSER").unwrap();
    sql.execute("SET search_path TO \"$user\", public").unwrap();
    assert!(sql.execute("SELECT count(*) FROM item").is_err());
    sql.execute("SET ROLE app").unwrap();
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM item"), 1);
    assert_eq!(int_at(&mut sql, "SELECT app.item_count()"), 1);
}

/// The documented restore sets the application role's search_path as a
/// role default, which applies to the role's later sessions.
#[test]
fn alter_role_set_records_session_defaults() {
    let (_dir, mut db) = session_db();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE ROLE app LOGIN").unwrap();
        sql.execute("ALTER ROLE app SET search_path TO app, pg_catalog")
            .expect("a list value is a valid role default");
        sql.execute("ALTER ROLE app SET statement_timeout = '5s'")
            .unwrap();
    }
    assert_eq!(
        bicdb_sql::role_session_settings(&db, "app").unwrap(),
        vec![
            ("search_path".to_string(), "app, pg_catalog".to_string()),
            ("statement_timeout".to_string(), "5s".to_string()),
        ]
    );
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("ALTER ROLE app RESET statement_timeout")
            .unwrap();
    }
    assert_eq!(
        bicdb_sql::role_session_settings(&db, "app").unwrap().len(),
        1
    );
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("ALTER ROLE app RESET ALL").unwrap();
    }
    assert!(bicdb_sql::role_session_settings(&db, "app")
        .unwrap()
        .is_empty());
}

/// pg_dump emits ALTER TABLE ... OWNER TO for views, and creates triggers on
/// schema-qualified tables.
#[test]
fn restored_view_owners_and_schema_qualified_triggers_work() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE SCHEMA app").unwrap();
    sql.execute("CREATE ROLE app_owner LOGIN").unwrap();
    sql.execute("CREATE VIEW app.due AS SELECT 1 AS due")
        .unwrap();
    sql.execute("ALTER TABLE app.due OWNER TO app_owner")
        .unwrap();
    assert_eq!(
        sql.execute("SELECT viewowner FROM pg_views WHERE viewname = 'due'")
            .unwrap()
            .rows[0][0]
            .to_cell(),
        "app_owner"
    );
    sql.execute("CREATE TABLE app.blob (id INT PRIMARY KEY)")
        .unwrap();
    sql.execute("CREATE TABLE app.removed (id INT)").unwrap();
    sql.execute(
        "CREATE FUNCTION app.note_removed() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN INSERT INTO app.removed VALUES (OLD.id); RETURN OLD; END; $$",
    )
    .unwrap();
    sql.execute(
        "CREATE TRIGGER note BEFORE DELETE ON app.blob FOR EACH ROW \
         EXECUTE FUNCTION app.note_removed()",
    )
    .expect("a schema-qualified trigger table resolves to that schema");
    sql.execute("INSERT INTO app.blob VALUES (7)").unwrap();
    sql.execute("DELETE FROM app.blob").unwrap();
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM app.removed"), 1);
/// Migration scripts register themselves with `SELECT f('..') FROM dual`,
/// where `f` writes; function bodies use `=` assignment and RAISE WARNING.
#[test]
fn writing_functions_in_select_lists_and_plpgsql_dialect_forms() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE public.registered (name TEXT)")
        .unwrap();
    sql.execute("CREATE VIEW public.one_row AS SELECT 'X' AS dummy")
        .unwrap();
    sql.execute(
        "CREATE FUNCTION public.register(p_name varchar) RETURNS void LANGUAGE plpgsql AS $$ \
         DECLARE n integer; BEGIN \
           n = 2; n = n * 3; \
           RAISE WARNING 'registering % (%)', p_name, n; \
           RAISE INFO 'info'; \
           INSERT INTO public.registered VALUES (p_name || n); \
         END; $$",
    )
    .unwrap();
    sql.execute("SELECT register('a') FROM one_row").unwrap();
    sql.execute("SELECT register(dummy) FROM one_row WHERE dummy = 'X'")
        .unwrap();
    let rows = sql
        .execute("SELECT name FROM public.registered ORDER BY name")
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![SqlValue::String("X6".into())],
            vec![SqlValue::String("a6".into())]
        ]
    );
}
