//! PostgreSQL transaction behaviour the WeWoBo owner API depends on, over the wire.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::net::TcpListener;

async fn count(client: &tokio_postgres::Client, sql: &str) -> String {
    let rows = client.simple_query(sql).await.unwrap();
    rows.iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .unwrap()
}

/// READ COMMITTED (the default) gives every statement of an explicit transaction the commits made
/// before it starts; REPEATABLE READ keeps the transaction's first snapshot.
#[test]
fn read_committed_statements_see_commits_made_during_the_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    std::thread::spawn(move || bicdb_pgwire::serve_existing_listener(server, listener));
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        let connect = || async {
            let (client, connection) = tokio_postgres::connect(
                &format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb"),
                tokio_postgres::NoTls,
            )
            .await
            .unwrap();
            tokio::spawn(connection);
            client
        };
        let reader = connect().await;
        let writer = connect().await;
        writer.simple_query("CREATE TABLE rc_rows (id int PRIMARY KEY, post_id int UNIQUE)").await.unwrap();
        reader.simple_query("BEGIN ISOLATION LEVEL READ COMMITTED").await.unwrap();
        assert_eq!(count(&reader, "SELECT count(*) FROM rc_rows").await, "0");
        writer.simple_query("INSERT INTO rc_rows VALUES (1, 1)").await.unwrap();
        assert_eq!(count(&reader, "SELECT count(*) FROM rc_rows").await, "1");
        // The search refresh shape: insert what is missing after another session indexed it.
        reader
            .simple_query("INSERT INTO rc_rows SELECT 1, 1 WHERE NOT EXISTS (SELECT 1 FROM rc_rows WHERE post_id = 1) ON CONFLICT (post_id) DO NOTHING")
            .await
            .unwrap();
        reader.simple_query("COMMIT").await.unwrap();

        reader.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY").await.unwrap();
        assert_eq!(count(&reader, "SELECT count(*) FROM rc_rows").await, "1");
        writer.simple_query("INSERT INTO rc_rows VALUES (2, 2)").await.unwrap();
        assert_eq!(count(&reader, "SELECT count(*) FROM rc_rows").await, "1");
        reader.simple_query("COMMIT").await.unwrap();
        assert_eq!(count(&reader, "SELECT count(*) FROM rc_rows").await, "2");
    });
}

/// Live delivery: an AFTER INSERT trigger calling pg_notify reaches LISTEN connections once the
/// inserting transaction commits (never before, never after a rollback).
#[test]
fn trigger_pg_notify_reaches_listeners_at_commit() {
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = PgWireServer::open(dir.path(), PgWireConfig::default()).unwrap();
    std::thread::spawn(move || bicdb_pgwire::serve_existing_listener(server, listener));
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        let url = format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb");
        let (writer, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(connection);
        let (reader, mut connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await.unwrap();
        let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut messages = futures_util::stream::poll_fn(move |cx| connection.poll_message(cx));
            use futures_util::StreamExt;
            while let Some(Ok(message)) = messages.next().await {
                if let tokio_postgres::AsyncMessage::Notification(note) = message {
                    let _ = sender.send(note.payload().to_string());
                }
            }
        });
        writer
            .batch_execute(
                "CREATE TABLE live_events (seq bigint PRIMARY KEY, kind text);
                 CREATE FUNCTION live_notify() RETURNS trigger LANGUAGE plpgsql AS $$
                 BEGIN PERFORM pg_notify('wewobo_live', NEW.seq::text); RETURN NEW; END $$;
                 CREATE TRIGGER live_events_notify AFTER INSERT ON live_events FOR EACH ROW EXECUTE FUNCTION live_notify();",
            )
            .await
            .unwrap();
        reader.batch_execute("LISTEN wewobo_live").await.unwrap();
        // Notifications reach a connection when it next talks to the server.
        writer.batch_execute("INSERT INTO live_events VALUES (1, 'message')").await.unwrap();
        writer.batch_execute("BEGIN; INSERT INTO live_events VALUES (2, 'message')").await.unwrap();
        reader.batch_execute("SELECT 1").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        reader.batch_execute("SELECT 1").await.unwrap();
        let mut seen = Vec::new();
        while let Ok(payload) = received.try_recv() {
            seen.push(payload);
        }
        assert_eq!(seen, vec!["1".to_string()], "the open transaction's event is not announced yet");
        writer.batch_execute("COMMIT").await.unwrap();
        writer.batch_execute("BEGIN; INSERT INTO live_events VALUES (3, 'message'); ROLLBACK").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        reader.batch_execute("SELECT 1").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        while let Ok(payload) = received.try_recv() {
            seen.push(payload);
        }
        assert_eq!(seen, vec!["1".to_string(), "2".to_string()]);
    });
}
