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
