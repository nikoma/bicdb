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
