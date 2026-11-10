use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, thread};
use tokio_postgres::NoTls;

// PostgreSQL 16 pins a fixed snapshot at the first command that needs one.
// Parse of a SELECT pins it even if no portal executes that statement.
#[tokio::test]
async fn first_snapshot_command_controls_peer_visibility() {
    let mut observations = Vec::new();
    let mut expected = Vec::new();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        drop(
            BicDb::open_with_config(
                root.path(),
                DbConfig::default().with_storage_mode(mode.clone()),
            )
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = PgWireServer::open(root.path(), PgWireConfig::default()).unwrap();
        let serving = server.clone();
        let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        let config = format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb");
        let (mut reader, reader_connection) =
            tokio_postgres::connect(&config, NoTls).await.unwrap();
        let reader_task = tokio::spawn(reader_connection);
        let (peer, peer_connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let peer_task = tokio::spawn(peer_connection);
        peer.batch_execute("CREATE TABLE snapshot_boundary(id integer PRIMARY KEY)")
            .await
            .unwrap();

        let mut id = 0i32;
        for begin in [
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; COMMIT AND CHAIN",
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; ROLLBACK AND CHAIN",
        ] {
            for (protocol, sql, visible) in [
                ("simple", "SHOW transaction_isolation", true),
                (
                    "simple",
                    "SET LOCAL application_name = 'snapshot_boundary'",
                    true,
                ),
                ("simple", "SAVEPOINT snapshot_boundary", true),
                ("simple", "SELECT 1", false),
                ("simple", "EXPLAIN SELECT 1", false),
                ("parse", "SELECT 1", false),
                ("parse", "SELECT id FROM snapshot_boundary", false),
            ] {
                reader.batch_execute(begin).await.unwrap();
                if protocol == "parse" {
                    let _prepared = reader.prepare(sql).await.unwrap();
                } else {
                    reader.batch_execute(sql).await.unwrap();
                }
                id += 1;
                peer.execute("INSERT INTO snapshot_boundary VALUES ($1)", &[&id])
                    .await
                    .unwrap();
                let found = reader
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM snapshot_boundary WHERE id=$1)",
                        &[&id],
                    )
                    .await
                    .unwrap()
                    .get::<_, bool>(0);
                let case = format!("{mode:?}: {begin}, {protocol} {sql}");
                observations.push((case.clone(), found));
                expected.push((case, visible));
                reader.batch_execute("ROLLBACK").await.unwrap();
            }
        }
        let statement = reader
            .prepare("SELECT id FROM snapshot_boundary WHERE id=$1")
            .await
            .unwrap();
        id += 1;
        let tx = reader
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await
            .unwrap();
        let portal = tx.bind(&statement, &[&id]).await.unwrap();
        peer.execute("INSERT INTO snapshot_boundary VALUES ($1)", &[&id])
            .await
            .unwrap();
        let found = !tx.query_portal(&portal, 0).await.unwrap().is_empty();
        let case = format!("{mode:?}: Bind of a statement prepared before BEGIN");
        observations.push((case.clone(), found));
        expected.push((case, false));
        tx.rollback().await.unwrap();
        drop(reader);
        drop(peer);
        reader_task.abort();
        peer_task.abort();
        server.request_shutdown();
        task.join().unwrap().unwrap();
    }
    assert_eq!(observations, expected);
}
