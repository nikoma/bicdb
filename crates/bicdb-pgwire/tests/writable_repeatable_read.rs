use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, thread, time::Duration};
use tokio_postgres::NoTls;

#[tokio::test]
async fn repeatable_read_rejects_concurrent_mutations_at_the_statement_boundary() {
    let mut failures = Vec::new();
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
        let (mut client, client_connection) =
            tokio_postgres::connect(&config, NoTls).await.unwrap();
        let client_task = tokio::spawn(client_connection);
        let (peer, peer_connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let peer_task = tokio::spawn(peer_connection);
        peer.batch_execute("CREATE TABLE rr_mutation(id integer PRIMARY KEY, k integer UNIQUE, v integer NOT NULL)")
            .await
            .unwrap();

        client
            .batch_execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ;
                 BEGIN; INSERT INTO rr_mutation VALUES (1, 100, 10); COMMIT",
            )
            .await
            .unwrap();
        let update_and_insert = "BEGIN; UPDATE rr_mutation SET v=11 WHERE id=1;
             INSERT INTO rr_mutation VALUES (2,200,12); COMMIT";
        let delete = "DELETE FROM rr_mutation WHERE id=1";
        for (concurrent, statement, expected_state) in [
            (update_and_insert, "UPDATE rr_mutation SET v=20 WHERE id=1 AND v=10", "40001"),
            (
                update_and_insert,
                "UPDATE rr_mutation AS target SET v=20 FROM (VALUES (1)) AS source(id)
                 WHERE target.id=source.id",
                "40001",
            ),
            (update_and_insert, "DELETE FROM rr_mutation WHERE id=1 AND v=10", "40001"),
            (update_and_insert, "SELECT v FROM rr_mutation WHERE id=1 FOR UPDATE", "40001"),
            (update_and_insert, "INSERT INTO rr_mutation VALUES (2,300,20)", "23505"),
            (update_and_insert, "INSERT INTO rr_mutation VALUES (2,300,20) ON CONFLICT DO NOTHING", "40001"),
            (update_and_insert, "INSERT INTO rr_mutation VALUES (2,300,20) ON CONFLICT(id) DO UPDATE SET v=EXCLUDED.v", "40001"),
            (update_and_insert, "INSERT INTO rr_mutation VALUES (1,100,20) ON CONFLICT(id) DO NOTHING", "40001"),
            (update_and_insert, "INSERT INTO rr_mutation VALUES (1,100,20) ON CONFLICT(id) DO UPDATE SET v=EXCLUDED.v WHERE false", "40001"),
            (delete, "INSERT INTO rr_mutation VALUES (1,100,20) ON CONFLICT(id) DO NOTHING", "00000"),
            (delete, "INSERT INTO rr_mutation VALUES (1,100,20)", "00000"),
            (update_and_insert, "INSERT INTO rr_mutation VALUES (3,200,20) ON CONFLICT DO NOTHING", "40001"),
            (update_and_insert, "INSERT INTO rr_mutation VALUES (3,200,20) ON CONFLICT(id) DO NOTHING", "23505"),
            (update_and_insert, "INSERT INTO rr_mutation VALUES (3,200,20) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v", "40001"),
        ] {
            peer.batch_execute("DELETE FROM rr_mutation; INSERT INTO rr_mutation VALUES (1,100,10)")
                .await
                .unwrap();
            client.batch_execute("BEGIN").await.unwrap();
            let original = client
                .query_one("SELECT v FROM rr_mutation WHERE id=1", &[])
                .await
                .unwrap();
            assert_eq!(original.get::<_, i32>(0), 10, "{mode:?}: {statement}");
            peer.batch_execute(concurrent).await.unwrap();
            let before: Vec<i32> = peer
                .query("SELECT v FROM rr_mutation ORDER BY id", &[])
                .await
                .unwrap()
                .iter()
                .map(|row| row.get(0))
                .collect();
            let result = client.batch_execute(statement).await;
            let state = match &result {
                Ok(()) => "00000",
                Err(error) => error.code().map_or("client error", |code| code.code()),
            };
            if state != expected_state {
                failures.push(format!("{mode:?}: {statement}: expected {expected_state}, got {state}"));
            }
            if state == "00000" && expected_state == "00000" {
                let row = client
                    .query_one("SELECT v FROM rr_mutation WHERE id=1", &[])
                    .await
                    .unwrap();
                if row.get::<_, i32>(0) != 20 {
                    failures.push(format!("{mode:?}: {statement}: reinserted row not visible"));
                }
            }
            client.batch_execute("ROLLBACK").await.unwrap();
            let after: Vec<i32> = peer
                .query("SELECT v FROM rr_mutation ORDER BY id", &[])
                .await
                .unwrap()
                .iter()
                .map(|row| row.get(0))
                .collect();
            assert_eq!(after, before, "{mode:?}: {statement}");
        }
        for (release, expected_state, expected_value) in
            [("COMMIT", "40001", 11), ("ROLLBACK", "00000", 20)]
        {
            peer.batch_execute("DELETE FROM rr_mutation").await.unwrap();
            client.batch_execute("BEGIN; SELECT 1").await.unwrap();
            peer.batch_execute("BEGIN; INSERT INTO rr_mutation VALUES (2,200,11)")
                .await
                .unwrap();
            let waiting = tokio::spawn(async move {
                let result = client
                    .batch_execute(
                        "INSERT INTO rr_mutation VALUES (2,300,20) ON CONFLICT DO NOTHING",
                    )
                    .await;
                (client, result)
            });
            let admitted = tokio::time::timeout(Duration::from_secs(2), async {
                while !waiting.is_finished()
                    && !server
                        .connection_snapshots()
                        .iter()
                        .any(|connection| connection.active_query.is_some())
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await;
            let waited_for_owner = admitted.is_ok() && !waiting.is_finished();
            peer.batch_execute(release).await.unwrap();
            let (returned, result) = tokio::time::timeout(Duration::from_secs(7), waiting)
                .await
                .unwrap()
                .unwrap();
            client = returned;
            assert!(
                waited_for_owner,
                "{mode:?}: competitor did not wait for {release}"
            );
            let state = match &result {
                Ok(()) => "00000",
                Err(error) => error.code().map_or("client error", |code| code.code()),
            };
            assert_eq!(state, expected_state, "{mode:?}: owner {release}");
            client.batch_execute("COMMIT").await.unwrap();
            let value = peer
                .query_one("SELECT v FROM rr_mutation WHERE id=2", &[])
                .await
                .unwrap()
                .get::<_, i32>(0);
            assert_eq!(value, expected_value, "{mode:?}: owner {release}");
        }
        drop(client);
        drop(peer);
        client_task.abort();
        peer_task.abort();
        server.request_shutdown();
        task.join().unwrap().unwrap();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
