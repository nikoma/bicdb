use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, thread, time::Duration};

#[tokio::test]
async fn sequence_allocation_progresses_while_another_transaction_waits_on_its_advisory_lock() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        root.path(),
        PgWireConfig {
            write_timeout: Duration::from_millis(500),
            ..Default::default()
        },
    )
    .unwrap();
    let serving = server.clone();
    let serving_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (first, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    let first_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    first.batch_execute("CREATE SEQUENCE progress_seq;CREATE TABLE sync_records(id bigserial PRIMARY KEY,label text);CREATE FUNCTION lock_patient(pkey int) RETURNS boolean LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(17,pkey); RETURN true; END $$").await.unwrap();
    let mut failures = Vec::new();
    for (index, statement) in [
        "SELECT nextval('progress_seq')",
        "INSERT INTO sync_records(label) VALUES('first'),('second')",
    ]
    .into_iter()
    .enumerate()
    {
        let key = 700 + index as i32;
        first.batch_execute("BEGIN").await.unwrap();
        first
            .query("SELECT pg_advisory_xact_lock(17,$1)", &[&key])
            .await
            .unwrap();
        let (second, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
            .await
            .unwrap();
        let second_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        second.batch_execute("BEGIN").await.unwrap();
        let waiting = tokio::spawn(async move {
            let result = second.query("SELECT lock_patient($1)", &[&key]).await;
            let commit = second.batch_execute("COMMIT").await;
            (result.is_ok(), commit.is_ok())
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !waiting.is_finished(),
            "the second transaction must wait on the patient lock"
        );
        let result = first.batch_execute(statement).await;
        if let Err(error) = &result {
            failures.push(format!("{statement}: {error}"));
        }
        // Always release the lock before asserting, including on the old engine.
        first
            .batch_execute(if result.is_ok() { "COMMIT" } else { "ROLLBACK" })
            .await
            .unwrap();
        let progress = tokio::time::timeout(Duration::from_secs(3), waiting)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(progress, (true, true));
        second_task.await.unwrap();
    }
    drop(first);
    first_task.await.unwrap();
    server.request_shutdown();
    serving_thread.join().unwrap().unwrap();
    assert!(failures.is_empty(),"sequence allocation must not need a global writer while a shared reader waits: {failures:?}");
}
