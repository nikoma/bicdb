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

// ---------------------------------------------------------------------------
// User-defined operators (`CREATE OPERATOR`), as a pg_dump of an application
// schema restores them: day arithmetic on timestamps through functions.
// ---------------------------------------------------------------------------

fn text_at(sql: &mut SqlSession, query: &str) -> String {
    match sql
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"))
        .rows[0][0]
    {
        SqlValue::String(ref value) => value.clone(),
        ref other => panic!("expected text from {query}, got {other:?}"),
    }
}

fn error_of(sql: &mut SqlSession, statement: &str) -> String {
    match sql.execute(statement) {
        Ok(result) => panic!("`{statement}` unexpectedly succeeded: {:?}", result.rows),
        Err(error) => error.to_string(),
    }
}

fn app_operator_count(sql: &mut SqlSession) -> i64 {
    int_at(
        sql,
        "SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid = o.oprnamespace \
         WHERE n.nspname = 'app'",
    )
}

/// Functions and operators in the shape a pg_dump writes them.
fn create_day_operators(sql: &mut SqlSession) {
    for statement in [
        "CREATE SCHEMA app",
        "CREATE FUNCTION app.adddays(datetime timestamp with time zone, days numeric) \
         RETURNS timestamp with time zone LANGUAGE plpgsql IMMUTABLE \
         AS $$ BEGIN RETURN datetime + make_interval(days => days::int); END; $$",
        "CREATE FUNCTION app.subtractdays(day timestamp with time zone, days numeric) \
         RETURNS timestamp with time zone LANGUAGE plpgsql IMMUTABLE \
         AS $$ BEGIN RETURN day - make_interval(days => days::int); END; $$",
        "CREATE FUNCTION app.intervaldays(inter interval, days numeric) RETURNS integer \
         LANGUAGE plpgsql IMMUTABLE \
         AS $$ BEGIN RETURN EXTRACT(DAY FROM inter)::int + days::int; END; $$",
        "CREATE OPERATOR app.+ (FUNCTION = app.intervaldays, LEFTARG = interval, \
         RIGHTARG = numeric, COMMUTATOR = OPERATOR(app.-))",
        "ALTER OPERATOR app.+ (interval, numeric) OWNER TO bicdb",
        "CREATE OPERATOR app.+ (FUNCTION = app.adddays, LEFTARG = timestamp with time zone, \
         RIGHTARG = numeric, COMMUTATOR = OPERATOR(app.+))",
        "ALTER OPERATOR app.+ (timestamp with time zone, numeric) OWNER TO bicdb",
        "CREATE OPERATOR app.- (FUNCTION = app.subtractdays, LEFTARG = timestamp with time zone, \
         RIGHTARG = numeric, COMMUTATOR = OPERATOR(app.-))",
        "ALTER OPERATOR app.- (timestamp with time zone, numeric) OWNER TO bicdb",
    ] {
        sql.execute(statement)
            .unwrap_or_else(|error| panic!("`{statement}` failed: {error}"));
    }
}

#[test]
fn create_operator_records_operators_and_commutator_shells() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    create_day_operators(&mut sql);

    // Three operators plus the three shells their COMMUTATOR options named.
    assert_eq!(app_operator_count(&mut sql), 6);
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid = o.oprnamespace \
             WHERE n.nspname = 'app' AND o.oprresult = 0"
        ),
        3,
        "commutator shells have no function or result"
    );
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid = o.oprnamespace \
             WHERE n.nspname = 'app' AND o.oprcom <> 0"
        ),
        6,
        "operators and shells link to each other"
    );
    // Built-in operators are in the catalog too.
    assert!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_operator WHERE oprnamespace = 11 AND oprname = '+'"
        ) > 30
    );

    // The pg_dump order then names `-`(numeric, interval) as the commutator
    // of a second operator although it already commutes with `+`. PostgreSQL
    // 17 and later reject that definition, and so its ALTER finds nothing.
    let error = error_of(
        &mut sql,
        "CREATE OPERATOR app.- (FUNCTION = app.intervaldays, LEFTARG = interval, \
         RIGHTARG = numeric, COMMUTATOR = OPERATOR(app.-))",
    );
    assert!(
        error.contains("commutator operator - is already the commutator of operator +"),
        "{error}"
    );
    let error = error_of(
        &mut sql,
        "ALTER OPERATOR app.- (interval, numeric) OWNER TO bicdb",
    );
    assert!(
        error.contains("operator does not exist: interval app.- numeric"),
        "{error}"
    );
    assert_eq!(
        app_operator_count(&mut sql),
        6,
        "a rejected definition writes nothing"
    );

    let error = error_of(
        &mut sql,
        "CREATE OPERATOR app.+ (FUNCTION = app.adddays, LEFTARG = timestamp with time zone, \
         RIGHTARG = numeric)",
    );
    assert!(error.contains("already exists"), "{error}");
    let error = error_of(
        &mut sql,
        "CREATE OPERATOR app.* (FUNCTION = app.adddays, LEFTARG = date, RIGHTARG = numeric)",
    );
    assert!(
        error.contains("function app.adddays(date, numeric) does not exist"),
        "{error}"
    );
    let error = error_of(
        &mut sql,
        "CREATE OPERATOR app.! (FUNCTION = app.adddays, LEFTARG = timestamp with time zone)",
    );
    assert!(
        error.contains("right argument type must be specified"),
        "{error}"
    );
}

#[test]
fn user_operators_resolve_by_search_path_and_operand_types() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    create_day_operators(&mut sql);
    sql.execute("CREATE TABLE app.docs (id int, dateacct timestamp, days numeric)")
        .unwrap();
    sql.execute("INSERT INTO app.docs VALUES (1, '2020-01-01 10:00:00', 3)")
        .unwrap();
    sql.execute("SET search_path TO app, pg_catalog").unwrap();

    // timestamptz + integer: the integer coerces to numeric.
    let value = text_at(
        &mut sql,
        "SELECT (timestamptz '2020-01-01 00:00:00+00' + 1)::text",
    );
    assert!(value.starts_with("2020-01-02 00:00:00"), "{value}");
    // timestamp coerces to timestamptz; a numeric column is the day count.
    let value = text_at(&mut sql, "SELECT (dateacct + 1)::text FROM app.docs");
    assert!(value.starts_with("2020-01-02 10:00:00"), "{value}");
    let value = text_at(&mut sql, "SELECT (dateacct - days)::text FROM app.docs");
    assert!(value.starts_with("2019-12-29 10:00:00"), "{value}");
    // date + integer is an exact built-in match and stays a date.
    assert_eq!(
        text_at(&mut sql, "SELECT (date '2020-01-01' + 1)::text"),
        "2020-01-02"
    );
    // ... while date + numeric has no built-in candidate.
    let value = text_at(&mut sql, "SELECT (date '2020-01-01' + 1.0)::text");
    assert!(value.starts_with("2020-01-02 00:00:00"), "{value}");
    // Built-in arithmetic is untouched.
    assert_eq!(int_at(&mut sql, "SELECT 2 + 3"), 5);
    assert_eq!(
        text_at(
            &mut sql,
            "SELECT (timestamptz '2020-01-01 00:00:00+00' + interval '1 day')::text"
        ),
        text_at(
            &mut sql,
            "SELECT (timestamptz '2020-01-02 00:00:00+00')::text"
        )
    );

    // Predicates and assignments resolve against the table's column types.
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM app.docs WHERE dateacct + 1 > timestamptz '2020-01-02 00:00:00+00'"
        ),
        1
    );
    sql.execute("UPDATE app.docs SET dateacct = dateacct + 1 WHERE id = 1")
        .unwrap();
    let value = text_at(&mut sql, "SELECT dateacct::text FROM app.docs");
    assert!(value.starts_with("2020-01-02 10:00:00"), "{value}");

    // A shell operator cannot be used; an unknown literal is ambiguous
    // between the interval and timestamp candidates of the same name.
    let error = error_of(
        &mut sql,
        "SELECT 1.5 + timestamptz '2020-01-01 00:00:00+00'",
    );
    assert!(error.contains("operator is only a shell"), "{error}");
    let error = error_of(&mut sql, "SELECT now() + '1'");
    assert!(error.contains("operator is not unique"), "{error}");

    // pg_dump's explicit form resolves without the schema on the path, in a
    // view as in a query; the view column takes the function's result type.
    sql.execute("SET search_path TO public").unwrap();
    let value = text_at(
        &mut sql,
        "SELECT (timestamptz '2020-01-01 00:00:00+00' OPERATOR(app.+) (2)::numeric)::text",
    );
    assert!(value.starts_with("2020-01-03 00:00:00"), "{value}");
    sql.execute(
        "CREATE VIEW app.due AS SELECT d.id, \
         ((d.dateacct)::timestamp with time zone OPERATOR(app.+) (30)::numeric) AS duedate \
         FROM app.docs d",
    )
    .unwrap();
    let result = sql.execute("SELECT duedate FROM app.due").unwrap();
    assert_eq!(result.column_types, vec![Some("timestamptz".to_string())]);
    let value = text_at(&mut sql, "SELECT duedate::text FROM app.due");
    assert!(value.starts_with("2020-02-01 10:00:00"), "{value}");
    let error = error_of(&mut sql, "SELECT 1 OPERATOR(app.+) 2");
    assert!(
        error.contains("operator does not exist: integer app.+ integer"),
        "{error}"
    );

    // Dropping an operator removes it from resolution and from its
    // commutator's link.
    sql.execute("DROP OPERATOR app.+ (timestamp with time zone, numeric)")
        .unwrap();
    assert_eq!(app_operator_count(&mut sql), 5);
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid = o.oprnamespace \
             WHERE n.nspname = 'app' AND o.oprcom <> 0"
        ),
        4
    );
    let error = error_of(
        &mut sql,
        "SELECT timestamptz '2020-01-01 00:00:00+00' OPERATOR(app.+) (2)::numeric",
    );
    assert!(error.contains("operator does not exist"), "{error}");
    sql.execute("DROP OPERATOR IF EXISTS app.+ (timestamp with time zone, numeric)")
        .unwrap();
}

#[test]
fn operator_ddl_rolls_back_with_its_transaction() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    create_day_operators(&mut sql);
    sql.execute("BEGIN").unwrap();
    sql.execute(
        "CREATE OPERATOR app.# (FUNCTION = app.adddays, LEFTARG = timestamp with time zone, \
         RIGHTARG = numeric, COMMUTATOR = OPERATOR(app.#))",
    )
    .unwrap();
    sql.execute("DROP OPERATOR app.- (timestamp with time zone, numeric)")
        .unwrap();
    assert_eq!(app_operator_count(&mut sql), 7);
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(app_operator_count(&mut sql), 6);
    sql.execute("SET search_path TO app, pg_catalog").unwrap();
    let value = text_at(
        &mut sql,
        "SELECT (timestamptz '2020-01-05 00:00:00+00' - 1)::text",
    );
    assert!(value.starts_with("2020-01-04 00:00:00"), "{value}");
}

#[test]
fn user_operators_survive_reopen_in_both_storage_modes() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let open = || {
            BicDb::open_with_config(
                dir.path(),
                DbConfig::default()
                    .with_fsync(false)
                    .with_storage_mode(mode.clone()),
            )
            .unwrap()
        };
        {
            let mut db = open();
            let mut sql = SqlSession::new(&mut db);
            create_day_operators(&mut sql);
        }
        let mut db = open();
        let mut sql = SqlSession::new(&mut db);
        assert_eq!(
            app_operator_count(&mut sql),
            6,
            "[{mode}] operators lost on reopen"
        );
        sql.execute("SET search_path TO app, pg_catalog").unwrap();
        let value = text_at(
            &mut sql,
            "SELECT (timestamptz '2020-01-01 00:00:00+00' + 7)::text",
        );
        assert!(value.starts_with("2020-01-08 00:00:00"), "[{mode}] {value}");
    }
}
