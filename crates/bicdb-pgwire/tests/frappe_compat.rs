//! Wire-protocol behaviour the Frappe Framework's PostgreSQL driver (psycopg2,
//! simple query protocol) relies on.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{
    net::TcpListener,
    thread,
    time::{Duration, Instant},
};
use tokio_postgres::{Client, SimpleQueryMessage};

async fn connect() -> (Client, tempfile::TempDir) {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = PgWireServer::open(root.path(), PgWireConfig::default()).unwrap();
    thread::spawn(move || bicdb_pgwire::serve_existing_listener(server, listener));
    let config = format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb");
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, root)
}

fn values(messages: &[SimpleQueryMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap_or("NULL").to_string()),
            _ => None,
        })
        .collect()
}

fn tag(messages: &[SimpleQueryMessage]) -> Option<u64> {
    messages.iter().find_map(|message| match message {
        SimpleQueryMessage::CommandComplete(count) => Some(*count),
        _ => None,
    })
}

/// `frappe.db.rollback()` re-opens a transaction and `begin(read_only=True)`
/// then sends START TRANSACTION READ ONLY inside it. PostgreSQL warns, keeps
/// the block, and applies the modes as SET TRANSACTION would.
#[tokio::test]
async fn begin_inside_a_transaction_warns_and_keeps_the_block() {
    let (client, _root) = connect().await;
    client
        .batch_execute("CREATE TABLE notes (id int PRIMARY KEY)")
        .await
        .unwrap();
    client.batch_execute("START TRANSACTION").await.unwrap();
    client
        .batch_execute("INSERT INTO notes VALUES (1)")
        .await
        .unwrap();
    // A bare BEGIN changes nothing: the block stays writable.
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute("INSERT INTO notes VALUES (2)")
        .await
        .unwrap();
    client.batch_execute("ROLLBACK").await.unwrap();
    // Frappe's sequence: rollback, START TRANSACTION, START TRANSACTION READ ONLY.
    client.batch_execute("START TRANSACTION").await.unwrap();
    client
        .batch_execute("START TRANSACTION READ ONLY")
        .await
        .unwrap();
    assert_eq!(
        values(
            &client
                .simple_query("SHOW transaction_read_only")
                .await
                .unwrap()
        ),
        vec!["on"]
    );
    let error = client
        .batch_execute("INSERT INTO notes VALUES (3)")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "25006");
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        values(
            &client
                .simple_query("SELECT count(*) FROM notes")
                .await
                .unwrap()
        ),
        vec!["0"]
    );
    // In a failed block BEGIN is refused like every other command.
    client.batch_execute("BEGIN").await.unwrap();
    assert!(client.batch_execute("SELECT 1/0").await.is_err());
    let error = client.batch_execute("BEGIN").await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "25P02");
    client.batch_execute("ROLLBACK").await.unwrap();
}

/// `frappe.db.set_execution_timeout()` uses `SET LOCAL statement_timeout`.
#[tokio::test]
async fn session_statement_timeout_cancels_long_statements() {
    let (client, _root) = connect().await;
    client
        .batch_execute("SET statement_timeout = 200")
        .await
        .unwrap();
    let started = Instant::now();
    let error = client
        .batch_execute("SELECT pg_sleep(5)")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "57014");
    assert!(started.elapsed() < Duration::from_secs(4));
    client
        .batch_execute("SET statement_timeout = 0")
        .await
        .unwrap();
    client.batch_execute("SELECT pg_sleep(0.3)").await.unwrap();

    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute("SET LOCAL statement_timeout = '250ms'")
        .await
        .unwrap();
    client.batch_execute("SAVEPOINT s").await.unwrap();
    let error = client
        .batch_execute("SELECT pg_sleep(5)")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "57014");
    client
        .batch_execute("ROLLBACK TO SAVEPOINT s")
        .await
        .unwrap();
    assert_eq!(
        values(&client.simple_query("SELECT 7").await.unwrap()),
        vec!["7"]
    );
    client.batch_execute("COMMIT").await.unwrap();
    // SET LOCAL ended with the transaction.
    client.batch_execute("SELECT pg_sleep(0.4)").await.unwrap();
}

/// psycopg2 named ("server-side") cursors, used by
/// `frappe.db.unbuffered_cursor()`.
#[tokio::test]
async fn declare_fetch_and_close_cursors() {
    let (client, _root) = connect().await;
    client
        .batch_execute(
            "CREATE TABLE country (name text PRIMARY KEY); \
             INSERT INTO country VALUES ('Chad'), ('Austria'), ('Benin')",
        )
        .await
        .unwrap();
    let error = client
        .batch_execute("DECLARE c CURSOR FOR SELECT name FROM country")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "25P01");

    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute(
            "DECLARE \"ss_cursor\" CURSOR WITHOUT HOLD FOR SELECT * FROM \"country\" ORDER BY name",
        )
        .await
        .unwrap();
    let first = client
        .simple_query("FETCH FORWARD 2 FROM \"ss_cursor\"")
        .await
        .unwrap();
    assert_eq!(values(&first), vec!["Austria", "Benin"]);
    assert_eq!(tag(&first), Some(2));
    let rest = client
        .simple_query("FETCH FORWARD 2 FROM \"ss_cursor\"")
        .await
        .unwrap();
    assert_eq!(values(&rest), vec!["Chad"]);
    assert!(values(
        &client
            .simple_query("FETCH FORWARD 2 FROM \"ss_cursor\"")
            .await
            .unwrap()
    )
    .is_empty());
    client.batch_execute("CLOSE \"ss_cursor\"").await.unwrap();
    let error = client
        .batch_execute("FETCH NEXT FROM \"ss_cursor\"")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "34000");
    client.batch_execute("ROLLBACK").await.unwrap();

    // WITHOUT HOLD cursors end with the transaction; WITH HOLD ones stay.
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute("DECLARE gone CURSOR FOR SELECT name FROM country ORDER BY name")
        .await
        .unwrap();
    client
        .batch_execute("DECLARE kept CURSOR WITH HOLD FOR SELECT name FROM country ORDER BY name")
        .await
        .unwrap();
    client.batch_execute("COMMIT").await.unwrap();
    assert!(client.batch_execute("FETCH gone").await.is_err());
    assert_eq!(
        values(&client.simple_query("FETCH ALL FROM kept").await.unwrap()),
        vec!["Austria", "Benin", "Chad"]
    );
    client.batch_execute("CLOSE ALL").await.unwrap();
}
