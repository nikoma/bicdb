use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, thread, time::Duration};

#[tokio::test]
async fn read_only_survives_wire_messages_prepared_execution_and_pool_reuse() {
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
    let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let config = format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb");
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let (other, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let other_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    client.batch_execute("CREATE TABLE guarded(id int PRIMARY KEY);INSERT INTO guarded VALUES(1);CREATE SEQUENCE guarded_seq START 5").await.unwrap();
    client.batch_execute("BEGIN READ ONLY").await.unwrap();
    assert_eq!(
        client
            .query_one("SHOW transaction_read_only", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "on"
    );
    let error = client
        .execute("INSERT INTO guarded VALUES($1)", &[&9i32])
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "25006");
    client.batch_execute("ROLLBACK").await.unwrap();
    other
        .execute("INSERT INTO guarded VALUES(2)", &[])
        .await
        .unwrap();
    client.batch_execute("BEGIN READ ONLY").await.unwrap();
    let error = client
        .query(
            "WITH changed AS (DELETE FROM guarded RETURNING id) SELECT count(*) FROM changed",
            &[],
        )
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "25006");
    client.batch_execute("ROLLBACK").await.unwrap();
    client.batch_execute("BEGIN READ ONLY").await.unwrap();
    let error = client
        .query("SELECT nextval('guarded_seq')", &[])
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "25006");
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        other
            .query_one("SELECT nextval('guarded_seq')", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        5
    );
    client
        .execute("INSERT INTO guarded VALUES(3)", &[])
        .await
        .unwrap();
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM guarded", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        3
    );
    drop(client);
    drop(other);
    connection_task.abort();
    other_task.abort();
    server.request_shutdown();
    task.join().unwrap().unwrap();
}

// PostgreSQL 16 reports READ UNCOMMITTED but uses a fresh committed snapshot
// for each statement. Exercise both wire protocols and every host entry path,
// including AND CHAIN, which must not accidentally pin this mode's snapshot.
#[tokio::test]
async fn read_uncommitted_uses_read_committed_visibility_in_both_storage_modes() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let root = tempfile::tempdir().unwrap();
        drop(
            bicdb_core::BicDb::open_with_config(
                root.path(),
                bicdb_core::DbConfig::default()
                    .with_fsync(false)
                    .with_storage_mode(mode.clone()),
            )
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = PgWireServer::open(
            root.path(),
            PgWireConfig {
                fsync: false,
                ..Default::default()
            },
        )
        .unwrap();
        let serving = server.clone();
        let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        let config = format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb");
        let (a, a_connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
            .await
            .unwrap();
        let a_task = tokio::spawn(a_connection);
        let (b, b_connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
            .await
            .unwrap();
        let b_task = tokio::spawn(b_connection);
        a.batch_execute("CREATE TABLE ru_rows(id integer PRIMARY KEY, v integer); INSERT INTO ru_rows VALUES(1,0)").await.unwrap();
        let prepared = a.prepare("SELECT v FROM ru_rows WHERE id=1").await.unwrap();
        for begin in [
            "BEGIN ISOLATION LEVEL READ UNCOMMITTED",
            "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            "BEGIN; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; BEGIN",
            "SET default_transaction_isolation='read uncommitted'; BEGIN",
            "BEGIN ISOLATION LEVEL READ UNCOMMITTED; COMMIT AND CHAIN",
            "BEGIN ISOLATION LEVEL READ UNCOMMITTED; ROLLBACK AND CHAIN",
        ] {
            b.batch_execute("UPDATE ru_rows SET v=0 WHERE id=1")
                .await
                .unwrap();
            a.batch_execute(begin).await.unwrap();
            assert_eq!(
                a.query_one("SHOW transaction_isolation", &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "read uncommitted",
                "{mode:?}: {begin}"
            );
            b.batch_execute("BEGIN; UPDATE ru_rows SET v=1 WHERE id=1")
                .await
                .unwrap();
            assert_eq!(
                a.query_one(&prepared, &[]).await.unwrap().get::<_, i32>(0),
                0,
                "dirty read: {mode:?}: {begin}"
            );
            b.batch_execute("COMMIT").await.unwrap();
            assert_eq!(
                a.query_one(&prepared, &[]).await.unwrap().get::<_, i32>(0),
                1,
                "pinned statement snapshot: {mode:?}: {begin}"
            );
            b.batch_execute("UPDATE ru_rows SET v=2 WHERE id=1")
                .await
                .unwrap();
            let rows = a
                .simple_query("SELECT v FROM ru_rows WHERE id=1")
                .await
                .unwrap();
            let value = rows.iter().find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
                _ => None,
            });
            assert_eq!(
                value,
                Some("2"),
                "simple protocol snapshot: {mode:?}: {begin}"
            );
            a.batch_execute("UPDATE ru_rows SET v=3 WHERE id=1")
                .await
                .unwrap();
            assert_eq!(
                a.query_one(&prepared, &[]).await.unwrap().get::<_, i32>(0),
                3
            );
            assert_eq!(
                b.query_one("SELECT v FROM ru_rows WHERE id=1", &[])
                    .await
                    .unwrap()
                    .get::<_, i32>(0),
                2
            );
            a.batch_execute("ROLLBACK; RESET default_transaction_isolation")
                .await
                .unwrap();
            assert_eq!(
                b.query_one("SELECT v FROM ru_rows WHERE id=1", &[])
                    .await
                    .unwrap()
                    .get::<_, i32>(0),
                2
            );
            assert_eq!(
                a.query_one("SHOW transaction_isolation", &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "read committed"
            );
        }
        drop(a);
        drop(b);
        a_task.await.unwrap().unwrap();
        b_task.await.unwrap().unwrap();
        server.request_shutdown();
        task.join().unwrap().unwrap();
    }
}
