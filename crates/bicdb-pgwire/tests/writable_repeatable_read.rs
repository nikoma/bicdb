use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, thread};
use tokio_postgres::NoTls;

#[tokio::test]
async fn repeatable_read_rejects_concurrent_mutations_at_the_statement_boundary() {
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
        let (client, client_connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let client_task = tokio::spawn(client_connection);
        let (peer, peer_connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let peer_task = tokio::spawn(peer_connection);
        peer.batch_execute("CREATE TABLE rr_mutation(id integer PRIMARY KEY, v integer NOT NULL)")
            .await
            .unwrap();

        client
            .batch_execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ;
                 BEGIN; INSERT INTO rr_mutation VALUES (1, 10); COMMIT",
            )
            .await
            .unwrap();
        for (statement, expected_state) in [
            ("UPDATE rr_mutation SET v=20 WHERE id=1 AND v=10", "40001"),
            (
                "UPDATE rr_mutation AS target SET v=20 FROM (VALUES (1)) AS source(id)
                 WHERE target.id=source.id",
                "40001",
            ),
            ("DELETE FROM rr_mutation WHERE id=1 AND v=10", "40001"),
            ("SELECT v FROM rr_mutation WHERE id=1 FOR UPDATE", "40001"),
            ("INSERT INTO rr_mutation VALUES (2,20)", "23505"),
            (
                "INSERT INTO rr_mutation VALUES (2,20) ON CONFLICT DO NOTHING",
                "40001",
            ),
            (
                "INSERT INTO rr_mutation VALUES (2,20) ON CONFLICT(id) DO UPDATE SET v=EXCLUDED.v",
                "40001",
            ),
        ] {
            peer.batch_execute("DELETE FROM rr_mutation; INSERT INTO rr_mutation VALUES (1,10)")
                .await
                .unwrap();
            client.batch_execute("BEGIN").await.unwrap();
            let original = client
                .query_one("SELECT v FROM rr_mutation WHERE id=1", &[])
                .await
                .unwrap();
            assert_eq!(original.get::<_, i32>(0), 10, "{mode:?}: {statement}");
            peer.batch_execute(
                "BEGIN; UPDATE rr_mutation SET v=11 WHERE id=1;
                 INSERT INTO rr_mutation VALUES (2,12); COMMIT",
            )
            .await
            .unwrap();
            let error = client.batch_execute(statement).await.unwrap_err();
            assert_eq!(
                error.code().unwrap().code(),
                expected_state,
                "{mode:?}: {statement}: {error:?}"
            );
            client.batch_execute("ROLLBACK").await.unwrap();
            let values: Vec<i32> = peer
                .query("SELECT v FROM rr_mutation ORDER BY id", &[])
                .await
                .unwrap()
                .iter()
                .map(|row| row.get(0))
                .collect();
            assert_eq!(values, [11, 12]);
        }
        drop(client);
        drop(peer);
        client_task.abort();
        peer_task.abort();
        server.request_shutdown();
        task.join().unwrap().unwrap();
    }
}
