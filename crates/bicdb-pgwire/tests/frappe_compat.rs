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
/// then sends START TRANSACTION inside it: PostgreSQL only warns.
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
    client
        .batch_execute("START TRANSACTION READ ONLY")
        .await
        .unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    // Still the same, writable block: the second BEGIN changed nothing.
    client
        .batch_execute("INSERT INTO notes VALUES (2)")
        .await
        .unwrap();
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
