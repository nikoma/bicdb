//! Internal access escalation must survive NOTICE and user exception handlers.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, thread, time::Duration};

#[tokio::test]
async fn routine_ddl_escalates_without_entering_user_handler_or_replaying_writes() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = PgWireServer::open(
        root.path(),
        PgWireConfig {
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .unwrap();
    let serving = server.clone();
    let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    client.batch_execute("CREATE TABLE escalation_rows(id integer PRIMARY KEY); CREATE TABLE escalation_target(id integer); CREATE FUNCTION escalation_probe() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN INSERT INTO escalation_rows VALUES(1); BEGIN RAISE NOTICE 'before DDL'; EXECUTE 'ALTER TABLE escalation_target ADD COLUMN extra integer'; EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION 'user handler caught: %', SQLERRM; END; RETURN 7; END $$").await.unwrap();
    client
        .batch_execute("BEGIN; INSERT INTO escalation_rows VALUES(100)")
        .await
        .unwrap();
    // Extended execution inside an existing transaction used to turn the
    // internal access request into a caught SQL error, preventing escalation.
    let result = client.query_one("SELECT escalation_probe()", &[]).await;
    let checked = if result.is_ok() {
        let count = client
            .query_one("SELECT count(*) FROM escalation_rows", &[])
            .await
            .unwrap()
            .get::<_, i64>(0);
        client.batch_execute("COMMIT").await.unwrap();
        let extra = client
            .query_one("SELECT extra FROM escalation_target", &[])
            .await
            .unwrap_err();
        // An empty target yields no row; resolving the new column succeeded.
        assert!(extra.code().is_none(), "new column must resolve: {extra}");
        count
    } else {
        client.batch_execute("ROLLBACK").await.unwrap();
        0
    };
    drop(client);
    connection_task.abort();
    server.request_shutdown();
    task.join().unwrap().unwrap();
    assert_eq!(result.unwrap().get::<_, i32>(0), 7);
    assert_eq!(
        checked, 2,
        "preserve earlier transaction writes and execute the routine insert once"
    );
}
