//! The durable prepare records of two-phase commit hold other transactions'
//! pending write sets: no SQL role, superuser included, can name, read or
//! write their collection, and no catalog lists it.
use bicdb_pgwire::{PgWireConfig, PgWireServer, ServerDatabaseOptions};
use std::{net::TcpListener, thread, time::Duration};

async fn connect(port: u16, user: &str) -> tokio_postgres::Client {
    let config = format!("host=127.0.0.1 port={port} user={user} dbname=bicdb");
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn prepare_records_are_not_a_relation_for_any_role() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = PgWireServer::open(
        root.path(),
        PgWireConfig {
            write_timeout: Duration::from_secs(2),
            database_options: ServerDatabaseOptions {
                max_prepared_transactions: Some(2),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let serving = server.clone();
    let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let admin = connect(port, "bicdb").await;
    admin
        .batch_execute(
            "CREATE ROLE p_reader LOGIN; CREATE TABLE secret(id int PRIMARY KEY, v text); \
             GRANT ALL ON secret TO p_reader; BEGIN; INSERT INTO secret VALUES (1, 'pending'); \
             PREPARE TRANSACTION 'hidden'",
        )
        .await
        .unwrap();
    let reader = connect(port, "p_reader").await;
    for client in [&admin, &reader] {
        for statement in [
            "SELECT * FROM __bicdb_prepared_xacts",
            "SELECT * FROM public.__bicdb_prepared_xacts",
            "INSERT INTO __bicdb_prepared_xacts VALUES ('x')",
            "UPDATE __bicdb_prepared_xacts SET id = 'x'",
            "DELETE FROM __bicdb_prepared_xacts",
            "COPY __bicdb_prepared_xacts TO STDOUT",
        ] {
            let error = client.batch_execute(statement).await.unwrap_err();
            let code = error.code().map(|code| code.code().to_string());
            assert_eq!(code.as_deref(), Some("42P01"), "{statement}: {error:?}");
        }
        for catalog in [
            "SELECT count(*) FROM pg_class WHERE relname = '__bicdb_prepared_xacts'",
            "SELECT count(*) FROM pg_tables WHERE tablename = '__bicdb_prepared_xacts'",
            "SELECT count(*) FROM information_schema.tables WHERE table_name = '__bicdb_prepared_xacts'",
            "SELECT count(*) FROM information_schema.columns WHERE table_name = '__bicdb_prepared_xacts'",
        ] {
            let count: i64 = client.query_one(catalog, &[]).await.unwrap().get(0);
            assert_eq!(count, 0, "{catalog}");
        }
    }
    // The pending row stays invisible; resolving still works.
    let count: i64 = reader
        .query_one("SELECT count(*) FROM secret", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    admin
        .batch_execute("COMMIT PREPARED 'hidden'")
        .await
        .unwrap();
    let count: i64 = reader
        .query_one("SELECT count(*) FROM secret", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
    drop(admin);
    drop(reader);
    server.request_shutdown();
    let _ = task.join();
}
