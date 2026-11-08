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

/// Dynamic SQL in a routine resolves unqualified relations through the
/// caller's search_path when it runs.
#[test]
fn dynamic_execute_binds_relations_through_search_path() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE SCHEMA app").unwrap();
    sql.execute("CREATE TABLE app.item (id INT, note TEXT)")
        .unwrap();
    sql.execute(
        "CREATE FUNCTION app.run(command text) RETURNS void LANGUAGE plpgsql AS $$ \
         BEGIN EXECUTE command; END; $$",
    )
    .unwrap();
    sql.execute("SET search_path TO app, public").unwrap();
    sql.execute("SELECT run('insert into item values (1, ''a'')')")
        .unwrap();
    sql.execute("SELECT run('alter table item alter column note set not null')")
        .unwrap();
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM app.item"), 1);
    assert!(sql.execute("INSERT INTO item VALUES (2, NULL)").is_err());
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

/// A correlated subquery names the outer table by its bare name, also when
/// the table lives in a schema (written qualified, or bound through
/// search_path).
#[test]
fn correlated_references_name_schema_qualified_tables_by_their_bare_name() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE SCHEMA app").unwrap();
    sql.execute("CREATE TABLE app.line (id INT, product_id INT, uom_id INT)")
        .unwrap();
    sql.execute("CREATE TABLE app.product (product_id INT, uom_id INT)")
        .unwrap();
    sql.execute("INSERT INTO app.line VALUES (1, 10, NULL), (2, 20, NULL)")
        .unwrap();
    sql.execute("INSERT INTO app.product VALUES (10, 100), (20, 200)")
        .unwrap();
    sql.execute(
        "UPDATE app.line SET uom_id = \
         (SELECT uom_id FROM app.product WHERE product_id = line.product_id)",
    )
    .unwrap();
    assert_eq!(int_at(&mut sql, "SELECT sum(uom_id) FROM app.line"), 300);
    sql.execute("SET search_path TO app, public").unwrap();
    sql.execute(
        "UPDATE Line SET uom_id = \
         (SELECT uom_id + 1 FROM Product WHERE product_id = Line.product_id)",
    )
    .unwrap();
    assert_eq!(int_at(&mut sql, "SELECT sum(uom_id) FROM line"), 302);
    sql.execute("DELETE FROM line WHERE EXISTS (SELECT 1 FROM product WHERE product.product_id = line.product_id AND product.uom_id = 200)")
        .unwrap();
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM line"), 1);
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
}

/// An unqualified CREATE FUNCTION creates (or replaces) the function in the
/// first schema on the search_path.
#[test]
fn unqualified_functions_are_created_in_the_first_search_path_schema() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE SCHEMA app").unwrap();
    sql.execute("CREATE FUNCTION app.make_id() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$")
        .unwrap();
    sql.execute("SET search_path TO app, public").unwrap();
    sql.execute(
        "CREATE OR REPLACE FUNCTION Make_ID() RETURNS integer LANGUAGE sql AS $$ SELECT 2 $$",
    )
    .unwrap();
    sql.execute(
        "CREATE FUNCTION fresh_fn() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RETURN 3; END $$",
    )
    .unwrap();
    let rows = sql
        .execute(
            "SELECT p.proname, n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE p.proname IN ('make_id', 'fresh_fn') ORDER BY 1",
        )
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![
            vec![
                SqlValue::String("fresh_fn".into()),
                SqlValue::String("app".into())
            ],
            vec![
                SqlValue::String("make_id".into()),
                SqlValue::String("app".into())
            ],
        ]
    );
    assert_eq!(int_at(&mut sql, "SELECT app.make_id()"), 2);
}

/// One simple-query text that sets search_path and then names relations
/// unqualified: each statement binds with the path and schemas in force when
/// it runs, not the path before the text started.
#[test]
fn statement_list_binds_each_statement_with_the_path_set_before_it() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE SCHEMA adempiere").unwrap();
    sql.execute("CREATE TABLE adempiere.ad_client (id INT PRIMARY KEY)")
        .unwrap();
    sql.execute("INSERT INTO adempiere.ad_client VALUES (11), (12)")
        .unwrap();
    assert_eq!(
        int_at(
            &mut sql,
            "SET search_path TO adempiere, public; SELECT count(*) FROM ad_client"
        ),
        2
    );
    // The path persists after the list, as a plain SET would.
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM ad_client"), 2);

    // A schema created earlier in the same text is on the path for the
    // statements after it, and new relations land in it.
    sql.execute("RESET search_path").unwrap();
    sql.execute(
        "CREATE SCHEMA batch_app; SET search_path TO batch_app, public; \
         CREATE TABLE batch_item (id INT); INSERT INTO batch_item VALUES (1)",
    )
    .unwrap();
    sql.execute("RESET search_path").unwrap();
    assert_eq!(
        int_at(&mut sql, "SELECT count(*) FROM batch_app.batch_item"),
        1
    );
    assert!(sql
        .execute("SELECT count(*) FROM public.batch_item")
        .is_err());

    // SET LOCAL lasts for the rest of the text, like an implicit transaction.
    assert_eq!(
        int_at(
            &mut sql,
            "SET LOCAL search_path TO adempiere, public; SELECT count(*) FROM ad_client"
        ),
        2
    );
    assert!(sql.execute("SELECT count(*) FROM ad_client").is_err());
}

/// current_schema and current_schemas report the search_path's schemas.
#[test]
fn current_schema_follows_the_search_path() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    let text = |sql: &mut SqlSession, query: &str| sql.execute(query).unwrap().rows[0][0].to_cell();
    assert_eq!(text(&mut sql, "SELECT current_schema()"), "public");
    sql.execute("CREATE SCHEMA app").unwrap();
    sql.execute("CREATE TABLE app.item (id INT)").unwrap();
    sql.execute("SET search_path TO app, public").unwrap();
    assert_eq!(text(&mut sql, "SELECT current_schema()"), "app");
    assert_eq!(text(&mut sql, "SELECT current_schema"), "app");
    assert_eq!(
        text(
            &mut sql,
            "SELECT array_to_string(current_schemas(false), ',')"
        ),
        "app,public"
    );
    assert_eq!(
        text(
            &mut sql,
            "SELECT array_to_string(current_schemas(true), ',')"
        ),
        "pg_catalog,app,public"
    );
    // Catalog lookups by CURRENT_SCHEMA inside a routine find the schema's relations.
    sql.execute(
        "CREATE FUNCTION app.has_item() RETURNS bigint LANGUAGE plpgsql AS $$ \
         DECLARE n bigint; BEGIN SELECT count(*) INTO n FROM pg_catalog.pg_class pc \
         WHERE pc.relname = 'item' AND pc.relnamespace = \
         (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = CURRENT_SCHEMA); \
         RETURN n; END; $$",
    )
    .unwrap();
    assert_eq!(int_at(&mut sql, "SELECT has_item()"), 1);
}

fn namespace_exists(sql: &mut SqlSession, name: &str) -> bool {
    int_at(
        sql,
        &format!("SELECT count(*) FROM pg_namespace WHERE nspname = '{name}'"),
    ) == 1
}

/// DROP SCHEMA follows PostgreSQL: RESTRICT (the default) refuses a schema
/// that holds objects; CASCADE drops them; IF EXISTS skips missing names; the
/// drop survives a restart.
#[test]
fn drop_schema_restrict_cascade_and_if_exists() {
    let dir = tempfile::tempdir().unwrap();
    let open = || {
        BicDb::open_with_config(
            dir.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(StorageMode::ServerPaged),
        )
        .unwrap()
    };
    {
        let mut db = open();
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE SCHEMA zz_empty").unwrap();
        sql.execute("DROP SCHEMA zz_empty").unwrap();
        assert!(!namespace_exists(&mut sql, "zz_empty"));
        let error = sql.execute("DROP SCHEMA zz_empty").unwrap_err();
        assert_eq!(error.sqlstate(), "3F000", "{error}");
        sql.execute("DROP SCHEMA IF EXISTS zz_empty, zz_never")
            .unwrap();

        for statement in [
            "CREATE SCHEMA zz_full",
            "CREATE TABLE zz_full.item (id INT PRIMARY KEY, label TEXT)",
            "INSERT INTO zz_full.item VALUES (1, 'one')",
            "CREATE INDEX item_label ON zz_full.item (label)",
            "CREATE SEQUENCE zz_full.item_seq",
            "CREATE VIEW zz_full.item_view AS SELECT id FROM zz_full.item",
            "CREATE TYPE zz_full.mood AS ENUM ('ok', 'sad')",
            "CREATE FUNCTION zz_full.item_total() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM zz_full.item'",
            "CREATE FUNCTION zz_full.item_trg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
            "CREATE TRIGGER item_audit BEFORE INSERT ON zz_full.item FOR EACH ROW EXECUTE FUNCTION zz_full.item_trg()",
            "CREATE SCHEMA zz_other",
            "CREATE TABLE zz_other.keep (id INT)",
        ] {
            sql.execute(statement).unwrap();
        }
        let error = sql.execute("DROP SCHEMA zz_full").unwrap_err();
        assert_eq!(error.sqlstate(), "2BP01", "{error}");
        let error = sql.execute("DROP SCHEMA zz_full RESTRICT").unwrap_err();
        assert_eq!(error.sqlstate(), "2BP01", "{error}");
        // A refused list drops nothing, not even the empty schema in it.
        sql.execute("CREATE SCHEMA zz_listed").unwrap();
        assert!(sql.execute("DROP SCHEMA zz_listed, zz_full").is_err());
        assert!(namespace_exists(&mut sql, "zz_listed"));
        assert_eq!(int_at(&mut sql, "SELECT count(*) FROM zz_full.item"), 1);

        sql.execute("DROP SCHEMA zz_full, zz_listed CASCADE")
            .unwrap();
        assert!(!namespace_exists(&mut sql, "zz_full"));
        assert!(!namespace_exists(&mut sql, "zz_listed"));
        assert!(sql.execute("SELECT count(*) FROM zz_full.item").is_err());
        assert!(sql
            .execute("SELECT count(*) FROM zz_full.item_view")
            .is_err());
        assert!(sql.execute("SELECT nextval('zz_full.item_seq')").is_err());
        assert!(sql.execute("SELECT zz_full.item_total()").is_err());
        assert_eq!(
            int_at(
                &mut sql,
                "SELECT count(*) FROM pg_trigger WHERE tgname = 'item_audit'"
            ),
            0
        );
        assert_eq!(
            int_at(
                &mut sql,
                "SELECT count(*) FROM pg_type WHERE typname = 'mood'"
            ),
            0
        );
        assert_eq!(int_at(&mut sql, "SELECT count(*) FROM zz_other.keep"), 0);
        // System schemas cannot be dropped.
        assert!(sql.execute("DROP SCHEMA pg_catalog").is_err());
        assert!(sql
            .execute("DROP SCHEMA information_schema CASCADE")
            .is_err());
        // The name is free again, and starts empty.
        sql.execute("CREATE SCHEMA zz_full").unwrap();
        sql.execute("CREATE TABLE zz_full.item (id INT)").unwrap();
        assert_eq!(int_at(&mut sql, "SELECT count(*) FROM zz_full.item"), 0);
        sql.execute("DROP SCHEMA zz_full CASCADE").unwrap();
    }
    let mut db = open();
    let mut sql = SqlSession::new(&mut db);
    assert!(!namespace_exists(&mut sql, "zz_full"));
    assert!(namespace_exists(&mut sql, "zz_other"));
    assert!(sql.execute("SELECT count(*) FROM zz_full.item").is_err());
}

/// Inside a transaction, DROP SCHEMA rolls back with everything it dropped.
#[test]
fn drop_schema_cascade_rolls_back_with_its_transaction() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE SCHEMA zz_tx").unwrap();
    sql.execute("CREATE TABLE zz_tx.item (id INT)").unwrap();
    sql.execute("INSERT INTO zz_tx.item VALUES (1)").unwrap();
    sql.execute("BEGIN").unwrap();
    sql.execute("DROP SCHEMA zz_tx CASCADE").unwrap();
    assert!(!namespace_exists(&mut sql, "zz_tx"));
    sql.execute("ROLLBACK").unwrap();
    assert!(namespace_exists(&mut sql, "zz_tx"));
    assert_eq!(int_at(&mut sql, "SELECT count(*) FROM zz_tx.item"), 1);
}

/// Only the schema owner (or a superuser) may drop a schema.
#[test]
fn drop_schema_requires_ownership() {
    let (_dir, mut db) = session_db();
    {
        let mut owner = SqlSession::new(&mut db);
        owner
            .execute("CREATE ROLE intruder LOGIN NOSUPERUSER")
            .unwrap();
        owner.execute("CREATE SCHEMA zz_owned").unwrap();
    }
    let mut intruder = SqlSession::new_unprivileged(&mut db, "intruder");
    let error = intruder.execute("DROP SCHEMA zz_owned").unwrap_err();
    assert!(format!("{error}").contains("owner"), "{error}");
}

#[test]
fn drop_schema_cascade_removes_every_overload() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE SCHEMA calc").unwrap();
    for signature in ["a integer", "a text", "a integer, b integer"] {
        sql.execute(&format!(
            "CREATE FUNCTION calc.twice({signature}) RETURNS text LANGUAGE sql AS $$ SELECT 'x' $$"
        ))
        .unwrap();
    }
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_proc WHERE proname = 'twice'"
        ),
        3
    );
    sql.execute("DROP SCHEMA calc CASCADE").unwrap();
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_proc WHERE proname = 'twice'"
        ),
        0
    );
    sql.execute("CREATE SCHEMA calc").unwrap();
    sql.execute(
        "CREATE FUNCTION calc.twice(a integer) RETURNS text LANGUAGE sql AS $$ SELECT 'y' $$",
    )
    .expect("no overload of the dropped schema survives");
}

/// A dump defines several functions of one name that differ by input types;
/// each call runs the overload whose declared types fit its arguments.
#[test]
fn overloaded_functions_restore_and_dispatch_by_argument_types() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE ROLE owner_role LOGIN").unwrap();
    sql.execute(
        "CREATE FUNCTION public.shift(inter interval, days numeric) RETURNS integer \
         LANGUAGE plpgsql AS $$ BEGIN RETURN EXTRACT(DAY FROM inter) + days; END; $$",
    )
    .unwrap();
    sql.execute(
        "CREATE FUNCTION public.shift(datetime timestamp with time zone, days numeric) \
         RETURNS date LANGUAGE plpgsql AS $$ \
         BEGIN RETURN CAST(datetime AS date) + CAST(days AS integer); END; $$",
    )
    .expect("a second signature is an overload, not a duplicate");
    sql.execute(
        "CREATE FUNCTION public.pick(character varying, integer) RETURNS text \
         LANGUAGE sql AS $$ SELECT 'text' $$",
    )
    .unwrap();
    sql.execute(
        "CREATE FUNCTION public.pick(numeric, numeric) RETURNS text \
         LANGUAGE sql AS $$ SELECT 'number' $$",
    )
    .unwrap();
    assert!(
        sql.execute(
            "CREATE FUNCTION public.pick(numeric, numeric) RETURNS text \
             LANGUAGE sql AS $$ SELECT 'again' $$",
        )
        .is_err(),
        "the same signature still conflicts"
    );

    let text = |sql: &mut SqlSession, query: &str| sql.execute(query).unwrap().rows[0][0].to_cell();
    assert_eq!(text(&mut sql, "SELECT shift(interval '3 days', 2)"), "5");
    assert_eq!(
        text(
            &mut sql,
            "SELECT shift(timestamptz '2026-01-30 10:00:00+00', 3)"
        ),
        "2026-02-02"
    );
    assert_eq!(
        text(
            &mut sql,
            "SELECT pg_typeof(shift(CAST(now() AS timestamptz), 1))"
        ),
        "date"
    );
    assert_eq!(text(&mut sql, "SELECT pick('a', 1)"), "text");
    assert_eq!(text(&mut sql, "SELECT pick(1.5, 2)"), "number");
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_proc WHERE proname IN ('shift', 'pick')"
        ),
        4
    );

    // DDL naming a signature acts on that overload, including unnamed ones.
    sql.execute("ALTER FUNCTION public.pick(character varying, integer) OWNER TO owner_role")
        .unwrap();
    sql.execute(
        "ALTER FUNCTION public.shift(timestamp with time zone, numeric) OWNER TO owner_role",
    )
    .unwrap();
    sql.execute("DROP FUNCTION public.shift(interval, numeric)")
        .unwrap();
    assert_eq!(
        text(
            &mut sql,
            "SELECT shift(timestamptz '2026-01-30 10:00:00+00', 1)"
        ),
        "2026-01-31"
    );
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_proc WHERE proname = 'shift'"
        ),
        1
    );
    sql.execute("BEGIN").unwrap();
    sql.execute("DROP FUNCTION public.pick(numeric, numeric)")
        .unwrap();
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(text(&mut sql, "SELECT pick(1.5, 2)"), "number");
    assert_eq!(text(&mut sql, "SELECT pick('a', 1)"), "text");
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
        // The pg_dump order names `-`(numeric, interval) as this operator's
        // commutator although it already commutes with `+`(interval, numeric).
        "CREATE OPERATOR app.- (FUNCTION = app.intervaldays, LEFTARG = interval, \
         RIGHTARG = numeric, COMMUTATOR = OPERATOR(app.-))",
        "ALTER OPERATOR app.- (interval, numeric) OWNER TO bicdb",
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

    // Four operators plus the three shells their COMMUTATOR options named.
    assert_eq!(app_operator_count(&mut sql), 7);
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
        7,
        "operators and shells link to each other"
    );
    // Built-in operators are in the catalog too.
    assert!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_operator WHERE oprnamespace = 11 AND oprname = '+'"
        ) > 30
    );

    // An already-paired commutator keeps its link (as PostgreSQL 16 does):
    // `-`(interval, numeric) points at the shell `-`(numeric, interval),
    // which still points back at `+`(interval, numeric).
    let linked = |sql: &mut SqlSession, left: i64, right: i64| {
        text_at(
            sql,
            &format!(
                "SELECT c.oprname || '(' || c.oprleft::text || ',' || c.oprright::text || ')' \
                 FROM pg_operator o JOIN pg_operator c ON c.oid = o.oprcom \
                 WHERE o.oprname = '-' AND o.oprleft = {left} AND o.oprright = {right}"
            ),
        )
    };
    assert_eq!(linked(&mut sql, 1186, 1700), "-(1700,1186)");
    assert_eq!(linked(&mut sql, 1700, 1186), "+(1186,1700)");

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
    assert_eq!(app_operator_count(&mut sql), 6);
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid = o.oprnamespace \
             WHERE n.nspname = 'app' AND o.oprcom <> 0"
        ),
        5
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
    assert_eq!(app_operator_count(&mut sql), 8);
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(app_operator_count(&mut sql), 7);
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
            7,
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

#[test]
fn drop_schema_restricts_on_and_cascades_to_operators() {
    let (_dir, mut db) = session_db();
    let mut sql = SqlSession::new(&mut db);
    create_day_operators(&mut sql);
    // A schema holding only operators (their function lives in `app`).
    sql.execute("CREATE SCHEMA ops").unwrap();
    sql.execute(
        "CREATE OPERATOR ops.+ (FUNCTION = app.adddays, LEFTARG = timestamp with time zone, \
         RIGHTARG = numeric, COMMUTATOR = OPERATOR(ops.+))",
    )
    .unwrap();
    let ops_count = |sql: &mut SqlSession| {
        int_at(
            sql,
            "SELECT count(*) FROM pg_operator o JOIN pg_namespace n ON n.oid = o.oprnamespace \
             WHERE n.nspname = 'ops'",
        )
    };
    assert_eq!(ops_count(&mut sql), 2);

    let error = sql.execute("DROP SCHEMA ops").unwrap_err();
    assert_eq!(error.sqlstate(), "2BP01", "{error}");
    assert!(error.to_string().contains("operator ops.+"), "{error}");
    assert_eq!(ops_count(&mut sql), 2);

    sql.execute("DROP SCHEMA ops CASCADE").unwrap();
    assert_eq!(ops_count(&mut sql), 0);

    // CASCADE drops a schema's operators together with their functions.
    sql.execute("DROP SCHEMA app CASCADE").unwrap();
    assert_eq!(app_operator_count(&mut sql), 0);
    assert_eq!(
        int_at(
            &mut sql,
            "SELECT count(*) FROM pg_operator WHERE oprnamespace <> 11"
        ),
        0
    );
}
