use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn text(value: &SqlValue) -> &str {
    match value {
        SqlValue::String(value) => value,
        other => panic!("expected text, got {other:?}"),
    }
}

#[test]
fn type_acls_match_postgres_defaults_and_enforce_usage() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session.execute("CREATE ROLE type_reader LOGIN").unwrap();
    session
        .execute("CREATE TYPE managed_state AS ENUM ('open', 'closed')")
        .unwrap();

    assert_eq!(
        session
            .execute("SELECT typacl FROM pg_type WHERE typname = 'managed_state'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]],
    );
    assert_eq!(
        session
            .execute("SELECT has_type_privilege('type_reader', 'managed_state', 'USAGE')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]],
    );

    session
        .execute("REVOKE USAGE ON TYPE managed_state FROM PUBLIC")
        .unwrap();
    let owner_acl = session
        .execute("SELECT typacl FROM pg_type WHERE typname = 'managed_state'")
        .unwrap()
        .rows;
    assert_eq!(owner_acl.len(), 1);
    assert!(text(&owner_acl[0][0]).contains("=U/"));
    assert_eq!(
        session
            .execute("SELECT has_type_privilege('type_reader', 'managed_state', 'USAGE')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]],
    );

    session.execute("SET ROLE type_reader").unwrap();
    let denied = session
        .execute("CREATE TABLE denied_type_use (id text PRIMARY KEY, state managed_state)")
        .unwrap_err();
    assert!(denied.to_string().contains("permission denied for type"));
    session.execute("RESET ROLE").unwrap();

    session
        .execute("GRANT USAGE ON TYPE managed_state TO type_reader")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT typacl FROM pg_type WHERE typname = 'managed_state'")
            .unwrap()
            .rows,
        vec![vec![SqlValue::String(
            "{bicdb=U/bicdb,type_reader=U/bicdb}".to_string(),
        )]],
    );
    session.execute("BEGIN").unwrap();
    session
        .execute("REVOKE USAGE ON TYPE managed_state FROM type_reader")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT has_type_privilege('type_reader', 'managed_state', 'USAGE')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(false)]],
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute("SELECT has_type_privilege('type_reader', 'managed_state', 'USAGE')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]],
    );
    session.execute("SET ROLE type_reader").unwrap();
    session
        .execute("CREATE TABLE allowed_type_use (id text PRIMARY KEY, state managed_state)")
        .unwrap();
    session.execute("RESET ROLE").unwrap();

    let oid = session
        .execute("SELECT oid FROM pg_type WHERE typname = 'managed_state'")
        .unwrap()
        .rows[0][0]
        .clone();
    assert_eq!(
        session
            .execute(&format!(
                "SELECT has_type_privilege('type_reader', {}, 'USAGE')",
                oid.to_cell()
            ))
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]],
    );
}

#[test]
fn comments_owner_and_schema_moves_are_durable_and_transactional() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session.execute("CREATE ROLE new_type_owner").unwrap();
    session.execute("GRANT new_type_owner TO bicdb").unwrap();
    session.execute("CREATE SCHEMA type_source").unwrap();
    session.execute("CREATE SCHEMA type_target").unwrap();
    session
        .execute("CREATE TYPE type_source.priority AS ENUM ('low', 'high')")
        .unwrap();
    session
        .execute("COMMENT ON TYPE type_source.priority IS 'workflow priority'")
        .unwrap();
    session
        .execute("CREATE TABLE typed_rows (id text PRIMARY KEY, priority type_source.priority)")
        .unwrap();
    session
        .execute(
            "CREATE FUNCTION echo_priority(value type_source.priority)
             RETURNS type_source.priority LANGUAGE sql AS 'SELECT value'",
        )
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("ALTER TYPE type_source.priority SET SCHEMA type_target")
        .unwrap();
    session
        .execute("ALTER TYPE type_target.priority RENAME TO moved_priority")
        .unwrap();
    assert_eq!(
        session
            .execute("SELECT pg_typeof(priority)::text FROM typed_rows")
            .unwrap()
            .rows,
        Vec::<Vec<SqlValue>>::new(),
    );
    session.execute("ROLLBACK").unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT typname, obj_description(oid, 'pg_type') FROM pg_type
                 WHERE typname = 'priority'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("priority".to_string()),
            SqlValue::String("workflow priority".to_string()),
        ]],
    );

    session
        .execute("ALTER TYPE type_source.priority SET SCHEMA type_target")
        .unwrap();
    session
        .execute("ALTER TYPE type_target.priority RENAME TO moved_priority")
        .unwrap();
    session
        .execute("ALTER TYPE type_target.moved_priority OWNER TO new_type_owner")
        .unwrap();
    let catalog = session
        .execute(
            "SELECT typname, typnamespace, typowner, obj_description(oid, 'pg_type')
             FROM pg_type WHERE typname = 'moved_priority'",
        )
        .unwrap()
        .rows;
    assert_eq!(catalog.len(), 1);
    assert_eq!(
        catalog[0][0],
        SqlValue::String("moved_priority".to_string())
    );
    assert_eq!(
        catalog[0][3],
        SqlValue::String("workflow priority".to_string())
    );
    assert_eq!(
        session
            .execute(
                "SELECT description FROM pg_description
                 WHERE objoid = (SELECT oid FROM pg_type WHERE typname = 'moved_priority')",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("workflow priority".to_string())]],
    );
    assert_eq!(
        session
            .execute(
                "SELECT pg_get_function_arguments(oid), pg_get_function_result(oid)
                 FROM pg_proc WHERE proname = 'echo_priority'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("value type_target.moved_priority".to_string()),
            SqlValue::String("type_target.moved_priority".to_string()),
        ]],
    );
    session
        .execute("COMMENT ON TYPE type_target.moved_priority IS NULL")
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT obj_description(oid, 'pg_type') FROM pg_type
                 WHERE typname = 'moved_priority'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Null]],
    );
}

#[test]
fn quoted_type_management_preserves_identifier_case() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session.execute(r#"CREATE SCHEMA "Type Source""#).unwrap();
    session.execute(r#"CREATE SCHEMA "Type.Target""#).unwrap();
    session.execute("CREATE ROLE quoted_type_reader").unwrap();
    session
        .execute(r#"CREATE TYPE "Type Source"."Managed.State" AS ENUM ('Ready')"#)
        .unwrap();
    session
        .execute(r#"REVOKE USAGE ON TYPE "Type Source"."Managed.State" FROM PUBLIC"#)
        .unwrap();
    session
        .execute(r#"GRANT USAGE ON TYPE "Type Source"."Managed.State" TO quoted_type_reader"#)
        .unwrap();
    assert_eq!(
        session
            .execute(
                r#"SELECT has_type_privilege('quoted_type_reader', '"Type Source"."Managed.State"', 'USAGE')"#,
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::Bool(true)]],
    );
    session
        .execute(r#"ALTER TYPE "Type Source"."Managed.State" SET SCHEMA "Type.Target""#)
        .unwrap();
    assert_eq!(
        session
            .execute(
                "SELECT t.typname, n.nspname
                 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
                 WHERE t.typname = 'Managed.State'",
            )
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("Managed.State".to_string()),
            SqlValue::String("Type.Target".to_string()),
        ]],
    );
}

#[test]
fn range_schema_moves_keep_the_automatic_multirange_in_its_original_schema() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session.execute("CREATE SCHEMA range_source").unwrap();
    session.execute("CREATE SCHEMA range_target").unwrap();
    session
        .execute("CREATE TYPE range_source.score_range AS RANGE (SUBTYPE = int4)")
        .unwrap();
    session
        .execute("ALTER TYPE range_source.score_range SET SCHEMA range_target")
        .unwrap();

    let rows = session
        .execute(
            "SELECT t.typname, n.nspname
             FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
             WHERE t.typname IN ('score_range', 'score_multirange')
             ORDER BY t.typname",
        )
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], SqlValue::String("score_multirange".to_string()));
    assert_eq!(rows[0][1], SqlValue::String("range_source".to_string()));
    assert_eq!(rows[1][0], SqlValue::String("score_range".to_string()));
    assert_eq!(rows[1][1], SqlValue::String("range_target".to_string()));
}

#[test]
fn dependencies_drive_restrict_and_cascade_and_are_cataloged() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut db);

    session
        .execute("CREATE TYPE dependency_state AS ENUM ('ready')")
        .unwrap();
    session
        .execute("CREATE DOMAIN dependency_domain AS dependency_state")
        .unwrap();
    session
        .execute("CREATE TABLE dependency_rows (id text PRIMARY KEY, state dependency_state)")
        .unwrap();

    let dependencies = session
        .execute(
            "SELECT classid, objid, refclassid, refobjid, deptype
             FROM pg_depend
             WHERE refclassid = 1247
               AND refobjid = (SELECT oid FROM pg_type WHERE typname = 'dependency_state')",
        )
        .unwrap()
        .rows;
    assert!(dependencies.len() >= 3, "dependencies: {dependencies:?}");

    let restricted = session.execute("DROP TYPE dependency_state").unwrap_err();
    assert!(restricted.to_string().contains("depend"));
    session
        .execute("DROP TYPE dependency_state CASCADE")
        .unwrap();
    assert!(session
        .execute(
            "SELECT typname FROM pg_type
                 WHERE typname IN ('dependency_state', 'dependency_domain')",
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn pg_dump_type_inventory_and_metadata_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE SCHEMA durable_types").unwrap();
        session.execute("CREATE ROLE durable_reader").unwrap();
        session
            .execute("CREATE TYPE durable_types.audit_state AS ENUM ('new', 'sealed')")
            .unwrap();
        session
            .execute("COMMENT ON TYPE durable_types.audit_state IS 'durable audit state'")
            .unwrap();
        session
            .execute("REVOKE USAGE ON TYPE durable_types.audit_state FROM PUBLIC")
            .unwrap();
        session
            .execute("GRANT USAGE ON TYPE durable_types.audit_state TO durable_reader")
            .unwrap();
        session
            .execute(
                "CREATE TABLE durable_audits (
                    id int4 PRIMARY KEY,
                    state durable_types.audit_state
                 )",
            )
            .unwrap();
        session
            .execute("INSERT INTO durable_audits VALUES (1, 'sealed')")
            .unwrap();
    }

    let mut reopened = BicDb::open(root.path()).unwrap();
    let mut session = SqlSession::new(&mut reopened);
    let inventory = session
        .execute(
            "SELECT tableoid, oid, typname, typnamespace, typacl,
                    acldefault('T', typowner) AS acldefault, typowner, typelem,
                    typrelid, typarray,
                    CASE WHEN typrelid = 0 THEN ' '::\"char\"
                         ELSE (SELECT relkind FROM pg_class WHERE oid = typrelid)
                    END AS typrelkind,
                    typtype, typisdefined,
                    typname[0] = '_' AND typelem != 0
                      AND (SELECT typarray FROM pg_type te WHERE oid = pg_type.typelem) = oid
                      AS isarray
             FROM pg_type
             WHERE typname IN ('audit_state', '_audit_state')
             ORDER BY typname",
        )
        .unwrap()
        .rows;
    assert_eq!(inventory.len(), 2);
    let base = inventory
        .iter()
        .find(|row| row[2] == SqlValue::String("audit_state".to_string()))
        .unwrap();
    let array = inventory
        .iter()
        .find(|row| row[2] == SqlValue::String("_audit_state".to_string()))
        .unwrap();
    assert!(matches!(base[4], SqlValue::String(_)));
    assert_eq!(base[11], SqlValue::String("e".to_string()));
    assert_eq!(base[13], SqlValue::Bool(false));
    assert_eq!(array[7], base[1]);
    assert_eq!(array[13], SqlValue::Bool(true));
    assert_eq!(
        session
            .execute("SELECT state, pg_typeof(state)::text FROM durable_audits ORDER BY id",)
            .unwrap()
            .rows,
        vec![vec![
            SqlValue::String("sealed".to_string()),
            SqlValue::String("durable_types.audit_state".to_string()),
        ]],
    );
    assert_eq!(
        session
            .execute(
                "SELECT obj_description(oid, 'pg_type') FROM pg_type
                 WHERE typname = 'audit_state'",
            )
            .unwrap()
            .rows,
        vec![vec![SqlValue::String("durable audit state".to_string())]],
    );
}
