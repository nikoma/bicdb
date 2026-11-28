//! A foreign key created while a referenced row's removal is still
//! uncommitted must not leave a referencing row uncovered. PostgreSQL's
//! SHARE ROW EXCLUSIVE lock on the referenced table makes the DDL wait;
//! BicDB's DDL cannot wait under its exclusive guard and refuses with
//! 40001 instead. Either way, no valid-looking constraint may cover an
//! orphan row.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, sync::Arc, thread, time::Duration};

async fn connect(port: u16) -> tokio_postgres::Client {
    let config = format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb");
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// One statement per message: the temporal-key DDL forms are taken one
/// statement at a time.
async fn run_each(client: &tokio_postgres::Client, statements: &str) {
    for statement in statements.split("; ") {
        client.batch_execute(statement).await.unwrap();
    }
}

fn code(result: &Result<(), tokio_postgres::Error>) -> Option<String> {
    result.as_ref().err().map(|error| {
        error
            .code()
            .map_or("none".into(), |code| code.code().to_string())
    })
}

/// The parent write is uncommitted when the key is created.
async fn parent_write_first(
    port: u16,
    setup: &str,
    parent_write: &str,
    add_key: &str,
    violations: &str,
    created_table: Option<&str>,
) {
    let owner = connect(port).await;
    run_each(&owner, setup).await;
    owner.batch_execute("BEGIN").await.unwrap();
    owner.batch_execute(parent_write).await.unwrap();
    let ddl = connect(port).await;
    let statement = add_key.to_string();
    let adding = tokio::spawn(async move {
        let result = ddl.batch_execute(&statement).await;
        (ddl, result)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let committed = owner.batch_execute("COMMIT").await;
    let (_ddl, added) = tokio::time::timeout(Duration::from_secs(20), adding)
        .await
        .expect("the DDL must finish")
        .unwrap();
    if let Some(code) = code(&added) {
        assert!(
            code == "40001" || code == "23503",
            "{add_key}: refused with {code}: {added:?}"
        );
    }
    let uncovered = if let Some(table) = created_table.filter(|_| added.is_err()) {
        let missing: bool = owner
            .query_one("SELECT to_regclass($1) IS NULL", &[&table])
            .await
            .unwrap()
            .get(0);
        assert!(missing, "{add_key}: failed batch left {table} behind");
        0
    } else {
        owner
            .query_one(violations, &[])
            .await
            .unwrap()
            .get::<_, i64>(0)
    };
    assert!(
        !(committed.is_ok() && added.is_ok() && uncovered > 0),
        "{add_key}: both committed and {uncovered} rows are uncovered"
    );
    assert!(
        committed.is_ok() || added.is_ok(),
        "{add_key}: at least one side must succeed: {committed:?} {added:?}"
    );
}

/// The key is created inside a still-open transaction when the parent row
/// is removed.
async fn key_first(port: u16, setup: &str, add_key: &str, parent_write: &str, violations: &str) {
    let ddl = connect(port).await;
    run_each(&ddl, setup).await;
    ddl.batch_execute("BEGIN").await.unwrap();
    ddl.batch_execute(add_key).await.unwrap();
    let owner = connect(port).await;
    let parent_write = parent_write.to_string();
    let writing = tokio::spawn(async move {
        let result = owner.batch_execute(&parent_write).await;
        (owner, result)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let added = ddl.batch_execute("COMMIT").await;
    let (owner, written) = tokio::time::timeout(Duration::from_secs(20), writing)
        .await
        .expect("the parent write must finish")
        .unwrap();
    let uncovered: i64 = owner.query_one(violations, &[]).await.unwrap().get(0);
    assert!(
        !(written.is_ok() && added.is_ok() && uncovered > 0),
        "{add_key}: both committed and {uncovered} rows are uncovered: {written:?} {added:?}"
    );
}

/// Both open at once: the key's transaction commits first, then the
/// transaction that removed a referenced row (planned before the key was
/// visible) commits.
async fn key_commits_before_parent_write(
    port: u16,
    setup: &str,
    add_key: &str,
    parent_write: &str,
    violations: &str,
) {
    let ddl = connect(port).await;
    run_each(&ddl, setup).await;
    ddl.batch_execute("BEGIN").await.unwrap();
    let added_statement = ddl.batch_execute(add_key).await;
    let owner = connect(port).await;
    owner.batch_execute("BEGIN").await.unwrap();
    let written_statement = owner.batch_execute(parent_write).await;
    let added = match added_statement {
        Ok(()) => ddl.batch_execute("COMMIT").await,
        Err(error) => {
            let _ = ddl.batch_execute("ROLLBACK").await;
            Err(error)
        }
    };
    let written = match written_statement {
        Ok(()) => owner.batch_execute("COMMIT").await,
        Err(error) => {
            let _ = owner.batch_execute("ROLLBACK").await;
            Err(error)
        }
    };
    for result in [&added, &written] {
        if let Some(code) = code(result) {
            assert!(
                code == "40001" || code == "23503",
                "{add_key}: refused with {code}: {result:?}"
            );
        }
    }
    let uncovered: i64 = owner.query_one(violations, &[]).await.unwrap().get(0);
    assert!(
        !(written.is_ok() && added.is_ok() && uncovered > 0),
        "{add_key}: both committed and {uncovered} rows are uncovered: {added:?} {written:?}"
    );
}

fn serve() -> (
    Arc<PgWireServer>,
    u16,
    thread::JoinHandle<()>,
    tempfile::TempDir,
) {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = PgWireServer::open(
        root.path(),
        PgWireConfig {
            write_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .unwrap();
    let serving = server.clone();
    let task = thread::spawn(move || {
        let _ = bicdb_pgwire::serve_existing_listener(serving, listener);
    });
    (server, port, task, root)
}

fn equality_setup(prefix: &str) -> String {
    format!(
        "CREATE TABLE {prefix}_parent(id int PRIMARY KEY); INSERT INTO {prefix}_parent VALUES (1), (2); \
         CREATE TABLE {prefix}_child(pid int); INSERT INTO {prefix}_child VALUES (1)"
    )
}

fn equality_violations(prefix: &str) -> String {
    format!(
        "SELECT count(*) FROM {prefix}_child c WHERE NOT EXISTS \
         (SELECT 1 FROM {prefix}_parent p WHERE p.id = c.pid) AND EXISTS \
         (SELECT 1 FROM pg_constraint WHERE conrelid = '{prefix}_child'::regclass AND contype = 'f')"
    )
}

fn temporal_setup(prefix: &str) -> String {
    format!(
        "CREATE TABLE {prefix}_parent (id int4range, valid_at int4range, \
         CONSTRAINT {prefix}_parent_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)); \
         INSERT INTO {prefix}_parent VALUES ('[1,2)', '[1,10)'), ('[2,3)', '[1,10)'); \
         CREATE TABLE {prefix}_child (id int4range, valid_at int4range); \
         INSERT INTO {prefix}_child VALUES ('[1,2)', '[2,5)')"
    )
}

fn temporal_violations(prefix: &str) -> String {
    format!(
        "SELECT count(*) FROM {prefix}_child c WHERE NOT EXISTS \
         (SELECT 1 FROM {prefix}_parent p WHERE p.id = c.id AND p.valid_at @> c.valid_at) AND EXISTS \
         (SELECT 1 FROM pg_constraint WHERE conrelid = '{prefix}_child'::regclass AND contype = 'f')"
    )
}

#[tokio::test]
async fn adding_an_equality_key_races_an_uncommitted_parent_delete() {
    let (server, port, task, _root) = serve();
    parent_write_first(
        port,
        &equality_setup("eq"),
        "DELETE FROM eq_parent WHERE id = 1",
        "ALTER TABLE eq_child ADD FOREIGN KEY (pid) REFERENCES eq_parent(id)",
        &equality_violations("eq"),
        None,
    )
    .await;
    parent_write_first(
        port,
        &equality_setup("equ"),
        "UPDATE equ_parent SET id = 9 WHERE id = 1",
        "ALTER TABLE equ_child ADD FOREIGN KEY (pid) REFERENCES equ_parent(id)",
        &equality_violations("equ"),
        None,
    )
    .await;
    // CREATE TABLE ... REFERENCES, with a referencing row in the same batch.
    parent_write_first(
        port,
        "CREATE TABLE cr_parent(id int PRIMARY KEY); INSERT INTO cr_parent VALUES (1)",
        "DELETE FROM cr_parent WHERE id = 1",
        "CREATE TABLE cr_child(pid int REFERENCES cr_parent(id)); INSERT INTO cr_child VALUES (1)",
        "SELECT count(*) FROM cr_child c WHERE NOT EXISTS (SELECT 1 FROM cr_parent p WHERE p.id = c.pid)",
        Some("cr_child"),
    )
    .await;
    // ATTACH PARTITION validates the attached rows against the parent's keys.
    parent_write_first(
        port,
        "CREATE TABLE ap_parent(id int PRIMARY KEY); INSERT INTO ap_parent VALUES (1), (2); \
         CREATE TABLE ap_child(pid int REFERENCES ap_parent(id)) PARTITION BY LIST (pid); \
         CREATE TABLE ap_part(pid int); INSERT INTO ap_part VALUES (1)",
        "DELETE FROM ap_parent WHERE id = 1",
        "ALTER TABLE ap_child ATTACH PARTITION ap_part FOR VALUES IN (1)",
        "SELECT count(*) FROM ap_child c WHERE NOT EXISTS (SELECT 1 FROM ap_parent p WHERE p.id = c.pid)",
        None,
    )
    .await;
    server.request_shutdown();
    let _ = task.join();
}

#[tokio::test]
async fn adding_a_temporal_key_races_an_uncommitted_parent_delete() {
    let (server, port, task, _root) = serve();
    parent_write_first(
        port,
        &temporal_setup("tp"),
        "DELETE FROM tp_parent WHERE id = '[1,2)'",
        "ALTER TABLE tp_child ADD FOREIGN KEY (id, PERIOD valid_at) REFERENCES tp_parent (id, PERIOD valid_at)",
        &temporal_violations("tp"),
        None,
    )
    .await;
    server.request_shutdown();
    let _ = task.join();
}

#[tokio::test]
async fn a_parent_delete_after_an_uncommitted_new_key_sees_it_or_conflicts() {
    let (server, port, task, _root) = serve();
    key_first(
        port,
        &equality_setup("rk"),
        "ALTER TABLE rk_child ADD FOREIGN KEY (pid) REFERENCES rk_parent(id)",
        "DELETE FROM rk_parent WHERE id = 1",
        &equality_violations("rk"),
    )
    .await;
    key_first(
        port,
        &temporal_setup("rt"),
        "ALTER TABLE rt_child ADD FOREIGN KEY (id, PERIOD valid_at) REFERENCES rt_parent (id, PERIOD valid_at)",
        "DELETE FROM rt_parent WHERE id = '[1,2)'",
        &temporal_violations("rt"),
    )
    .await;
    server.request_shutdown();
    let _ = task.join();
}

#[tokio::test]
async fn a_key_committed_first_still_holds_over_an_earlier_planned_parent_delete() {
    let (server, port, task, _root) = serve();
    key_commits_before_parent_write(
        port,
        &equality_setup("ck"),
        "ALTER TABLE ck_child ADD FOREIGN KEY (pid) REFERENCES ck_parent(id)",
        "DELETE FROM ck_parent WHERE id = 1",
        &equality_violations("ck"),
    )
    .await;
    key_commits_before_parent_write(
        port,
        &temporal_setup("ct"),
        "ALTER TABLE ct_child ADD FOREIGN KEY (id, PERIOD valid_at) REFERENCES ct_parent (id, PERIOD valid_at)",
        "DELETE FROM ct_parent WHERE id = '[1,2)'",
        &temporal_violations("ct"),
    )
    .await;
    server.request_shutdown();
    let _ = task.join();
}

#[tokio::test]
async fn an_unrelated_parent_writer_does_not_block_a_key_on_another_table() {
    let (server, port, task, _root) = serve();
    let owner = connect(port).await;
    owner
        .batch_execute(&format!(
            "{}; CREATE TABLE other(id int PRIMARY KEY); INSERT INTO other VALUES (1)",
            equality_setup("un")
        ))
        .await
        .unwrap();
    owner
        .batch_execute("BEGIN; DELETE FROM other WHERE id = 1")
        .await
        .unwrap();
    let ddl = connect(port).await;
    ddl.batch_execute("ALTER TABLE un_child ADD FOREIGN KEY (pid) REFERENCES un_parent(id)")
        .await
        .unwrap();
    owner.batch_execute("COMMIT").await.unwrap();
    server.request_shutdown();
    let _ = task.join();
}
