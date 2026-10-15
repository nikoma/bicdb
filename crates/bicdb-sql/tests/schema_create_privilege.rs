//! CREATE privilege on a schema.
//!
//! Found during a staging install: after the owner ran
//! `REVOKE CREATE ON SCHEMA public FROM PUBLIC`, a runtime login holding only
//! DML grants could still `CREATE TABLE` in `public`. ALTER on existing tables
//! was correctly refused (ownership gate), but nothing ever consulted the
//! schema's CREATE privilege, so the REVOKE was a silent no-op.
//!
//! PostgreSQL requires CREATE on the target schema (or ownership of it, or
//! superuser) for every object created in a schema: tables, views,
//! materialized views, sequences, types, domains, functions and procedures.
//! BicDB keeps the pre-15 default where PUBLIC may create in `public`, so
//! existing deployments keep working, but an explicit REVOKE now takes effect.

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn db() -> (tempfile::TempDir, BicDb) {
    let directory = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    (directory, db)
}

/// The staging foundation: the owner migrates, creates a runtime login with
/// DML only, and takes CREATE on `public` away from PUBLIC.
fn locked_down() -> (tempfile::TempDir, BicDb) {
    let (directory, mut db) = db();
    {
        let mut owner = SqlSession::new(&mut db);
        for sql in [
            "CREATE TABLE bot_tasks (id TEXT PRIMARY KEY, state TEXT)",
            "CREATE ROLE bot_runtime LOGIN",
            "REVOKE CREATE ON SCHEMA public FROM PUBLIC",
            "GRANT USAGE ON SCHEMA public TO bot_runtime",
            "GRANT SELECT, INSERT, UPDATE ON bot_tasks TO bot_runtime",
        ] {
            owner
                .execute(sql)
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
        }
    }
    (directory, db)
}

fn assert_denied(session: &mut SqlSession<'_>, sql: &str) {
    let error = session
        .execute(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} must be refused"));
    let rendered = format!("{error}");
    assert!(
        rendered.contains("permission denied for schema") || rendered.contains("owner"),
        "{sql}: unexpected refusal: {rendered}"
    );
}

const CREATE_STATEMENTS: &[&str] = &[
    "CREATE TABLE unauthorized (id TEXT)",
    "CREATE TABLE IF NOT EXISTS unauthorized (id TEXT)",
    "CREATE TABLE public.unauthorized (id TEXT)",
    "CREATE TABLE unauthorized_like (LIKE bot_tasks)",
    "CREATE VIEW unauthorized_view AS SELECT id FROM bot_tasks",
    "CREATE MATERIALIZED VIEW unauthorized_matview AS SELECT id FROM bot_tasks",
    "CREATE SEQUENCE unauthorized_seq",
    "CREATE TYPE unauthorized_mood AS ENUM ('ok', 'bad')",
    "CREATE TYPE unauthorized_pair AS (a INT, b INT)",
    "CREATE DOMAIN unauthorized_positive AS INT CHECK (VALUE > 0)",
    "CREATE FUNCTION unauthorized_fn() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
    "CREATE PROCEDURE unauthorized_proc() LANGUAGE SQL AS $$ SELECT 1 $$",
];

#[test]
fn revoked_public_create_refuses_every_object_kind() {
    let (_directory, mut db) = locked_down();
    {
        let mut runtime = SqlSession::new_unprivileged(&mut db, "bot_runtime");
        for sql in CREATE_STATEMENTS {
            assert_denied(&mut runtime, sql);
        }
        // DML and ALTER behave exactly as before.
        runtime
            .execute("INSERT INTO bot_tasks VALUES ('t1', 'queued')")
            .unwrap();
        assert_denied(
            &mut runtime,
            "ALTER TABLE bot_tasks ADD COLUMN unauthorized TEXT",
        );
    }
    let mut owner = SqlSession::new(&mut db);
    for relation in ["unauthorized", "unauthorized_like", "unauthorized_view"] {
        assert!(
            owner.execute(&format!("SELECT * FROM {relation}")).is_err(),
            "{relation} must not have been created"
        );
    }
}

/// The REVOKE has to be durable state, not a session-local flag.
#[test]
fn revocation_survives_reopen() {
    let (directory, db) = locked_down();
    drop(db);
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    let mut runtime = SqlSession::new_unprivileged(&mut db, "bot_runtime");
    assert_denied(&mut runtime, "CREATE TABLE unauthorized (id TEXT)");
}

/// The default is unchanged: without a REVOKE, PUBLIC may still create in
/// `public`, as every existing BicDB deployment and PostgreSQL <= 14 expect.
#[test]
fn default_public_schema_still_allows_create() {
    let (_directory, mut db) = db();
    SqlSession::new(&mut db)
        .execute("CREATE ROLE app LOGIN")
        .unwrap();
    let mut app = SqlSession::new_unprivileged(&mut db, "app");
    for sql in [
        "CREATE TABLE app_items (id TEXT PRIMARY KEY)",
        "CREATE VIEW app_view AS SELECT id FROM app_items",
        "CREATE SEQUENCE app_seq",
        "CREATE TYPE app_mood AS ENUM ('ok')",
        "CREATE FUNCTION app_fn() RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$",
    ] {
        app.execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

/// The owner/migration login keeps migrating after the lockdown.
#[test]
fn superuser_owner_still_creates_after_revoke() {
    let (_directory, mut db) = locked_down();
    let mut owner = SqlSession::new(&mut db);
    // Skip the spellings that re-create the first table under the same name.
    for sql in CREATE_STATEMENTS
        .iter()
        .filter(|sql| !sql.contains("IF NOT EXISTS") && !sql.contains("public."))
    {
        owner
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[test]
fn explicit_grant_restores_create_and_revoke_removes_it() {
    let (_directory, mut db) = locked_down();
    SqlSession::new(&mut db)
        .execute("GRANT CREATE ON SCHEMA public TO bot_runtime")
        .unwrap();
    SqlSession::new_unprivileged(&mut db, "bot_runtime")
        .execute("CREATE TABLE granted (id TEXT)")
        .unwrap();
    SqlSession::new(&mut db)
        .execute("REVOKE CREATE ON SCHEMA public FROM bot_runtime")
        .unwrap();
    let mut runtime = SqlSession::new_unprivileged(&mut db, "bot_runtime");
    assert_denied(&mut runtime, "CREATE TABLE regranted (id TEXT)");
}

#[test]
fn grant_to_public_restores_the_default() {
    let (_directory, mut db) = locked_down();
    SqlSession::new(&mut db)
        .execute("GRANT CREATE ON SCHEMA public TO PUBLIC")
        .unwrap();
    SqlSession::new_unprivileged(&mut db, "bot_runtime")
        .execute("CREATE TABLE public_again (id TEXT)")
        .unwrap();
}

#[test]
fn create_privilege_is_inherited_through_role_membership() {
    let (_directory, mut db) = locked_down();
    {
        let mut owner = SqlSession::new(&mut db);
        for sql in [
            "CREATE ROLE schema_writers NOLOGIN",
            "CREATE ROLE migrator LOGIN",
            "GRANT CREATE ON SCHEMA public TO schema_writers",
            "GRANT schema_writers TO migrator",
        ] {
            owner
                .execute(sql)
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
        }
    }
    SqlSession::new_unprivileged(&mut db, "migrator")
        .execute("CREATE TABLE inherited (id TEXT)")
        .unwrap();
    let mut runtime = SqlSession::new_unprivileged(&mut db, "bot_runtime");
    assert_denied(&mut runtime, "CREATE TABLE not_inherited (id TEXT)");
}

/// PostgreSQL 15+: `public` belongs to `pg_database_owner`, so whoever owns
/// the database may create in it regardless of PUBLIC's privileges.
#[test]
fn database_owner_creates_in_public_after_revoke() {
    let (_directory, mut db) = locked_down();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("CREATE ROLE app_owner LOGIN").unwrap();
        owner
            .execute("ALTER DATABASE bicdb OWNER TO app_owner")
            .unwrap();
    }
    SqlSession::new_unprivileged(&mut db, "app_owner")
        .execute("CREATE TABLE owned_by_db_owner (id TEXT)")
        .unwrap();
}

#[test]
fn schema_owner_creates_and_others_need_a_grant() {
    let (_directory, mut db) = db();
    {
        let mut owner = SqlSession::new(&mut db);
        for sql in [
            "CREATE ROLE tenant LOGIN",
            "CREATE ROLE outsider LOGIN",
            "CREATE SCHEMA tenant_data AUTHORIZATION tenant",
        ] {
            owner
                .execute(sql)
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
        }
    }
    SqlSession::new_unprivileged(&mut db, "tenant")
        .execute("CREATE TABLE tenant_data.items (id TEXT)")
        .unwrap();
    {
        let mut outsider = SqlSession::new_unprivileged(&mut db, "outsider");
        assert_denied(&mut outsider, "CREATE TABLE tenant_data.intruder (id TEXT)");
        assert_denied(&mut outsider, "CREATE SEQUENCE tenant_data.intruder_seq");
        assert_denied(
            &mut outsider,
            "CREATE VIEW tenant_data.intruder_view AS SELECT 1 AS one",
        );
    }
    SqlSession::new_unprivileged(&mut db, "tenant")
        .execute("GRANT CREATE ON SCHEMA tenant_data TO outsider")
        .unwrap();
    SqlSession::new_unprivileged(&mut db, "outsider")
        .execute("CREATE TABLE tenant_data.invited (id TEXT)")
        .unwrap();
}

/// A transaction that fails on the privilege check must leave nothing behind
/// and REVOKE inside a rolled-back transaction must not stick.
#[test]
fn rolled_back_revoke_restores_the_default() {
    let (_directory, mut db) = db();
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("CREATE ROLE app LOGIN").unwrap();
        owner.execute("BEGIN").unwrap();
        owner
            .execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
            .unwrap();
        owner.execute("ROLLBACK").unwrap();
    }
    SqlSession::new_unprivileged(&mut db, "app")
        .execute("CREATE TABLE still_allowed (id TEXT)")
        .unwrap();
}

#[test]
fn has_schema_privilege_reports_create() {
    let (_directory, mut db) = locked_down();
    let mut owner = SqlSession::new(&mut db);
    owner.execute("CREATE ROLE fresh LOGIN").unwrap();
    let rows = owner
        .execute(
            "SELECT has_schema_privilege('bot_runtime', 'public', 'CREATE'), \
                    has_schema_privilege('bicdb', 'public', 'CREATE')",
        )
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![vec![SqlValue::Bool(false), SqlValue::Bool(true)]]
    );
}

/// Without this, `CREATE TABLE fresh_schema.t` (which creates the schema on
/// the fly) or `CREATE SCHEMA` would sidestep a locked-down `public`.
/// Creating a schema needs CREATE on the database; like `public`, BicDB keeps
/// PUBLIC's historical default and honors an explicit REVOKE.
#[test]
fn revoked_database_create_refuses_new_schemas() {
    let (_directory, mut db) = locked_down();
    SqlSession::new(&mut db)
        .execute("REVOKE CREATE ON DATABASE bicdb FROM PUBLIC")
        .unwrap();
    {
        let mut runtime = SqlSession::new_unprivileged(&mut db, "bot_runtime");
        for sql in [
            "CREATE SCHEMA runtime_schema",
            "CREATE TABLE runtime_schema.t (id TEXT)",
            "CREATE TABLE runtime_schema_too.t (id TEXT)",
        ] {
            let error = runtime
                .execute(sql)
                .err()
                .unwrap_or_else(|| panic!("{sql} must be refused"));
            assert!(
                format!("{error}").contains("permission denied for"),
                "{sql}: {error}"
            );
        }
    }
    {
        let mut owner = SqlSession::new(&mut db);
        owner.execute("CREATE SCHEMA owner_schema").unwrap();
        owner
            .execute("GRANT CREATE ON DATABASE bicdb TO bot_runtime")
            .unwrap();
    }
    SqlSession::new_unprivileged(&mut db, "bot_runtime")
        .execute("CREATE SCHEMA runtime_schema")
        .unwrap();
}

#[test]
fn default_database_create_still_allows_new_schemas() {
    let (_directory, mut db) = db();
    SqlSession::new(&mut db)
        .execute("CREATE ROLE app LOGIN")
        .unwrap();
    let mut app = SqlSession::new_unprivileged(&mut db, "app");
    app.execute("CREATE SCHEMA app_schema").unwrap();
    app.execute("CREATE TABLE app_schema.items (id TEXT)")
        .unwrap();
    app.execute("CREATE TABLE implicit_schema.items (id TEXT)")
        .unwrap();
}
