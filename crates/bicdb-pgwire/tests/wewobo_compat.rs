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
