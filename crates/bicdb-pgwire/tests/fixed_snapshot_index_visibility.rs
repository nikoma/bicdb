use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, thread};
use tokio_postgres::NoTls;

#[tokio::test]
async fn fixed_snapshots_retain_rows_removed_from_live_index_candidates() {
    verify_fixed_index_visibility(
        "postgres",
        &[
            "REPEATABLE READ",
            "REPEATABLE READ READ ONLY",
            "SERIALIZABLE",
            "SERIALIZABLE READ ONLY",
        ],
    )
    .await;
}

#[tokio::test]
async fn oracle_snapshot_isolation_retains_historical_index_candidates() {
    verify_fixed_index_visibility(
        "oracle-preview-v1",
        &["SERIALIZABLE", "SERIALIZABLE READ ONLY"],
    )
    .await;
}

async fn verify_fixed_index_visibility(profile: &str, levels: &[&str]) {
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
        let server = PgWireServer::open(
            root.path(),
            PgWireConfig {
                sql_profile: profile.into(),
                ..Default::default()
            },
        )
        .unwrap();
        let serving = server.clone();
        let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        let config = format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb");
        let (reader, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let reader_task = tokio::spawn(connection);
        let (writer, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let writer_task = tokio::spawn(connection);
        writer
            .batch_execute(
                "CREATE TABLE history(id INT PRIMARY KEY,k INT,body JSONB);
                 CREATE INDEX history_k ON history(k);
                 CREATE INDEX history_body ON history USING gin(body)",
            )
            .await
            .unwrap();
        for isolation in levels {
            writer
                .batch_execute(
                    "DELETE FROM history;
                     INSERT INTO history VALUES (1,10,'{\"status\":\"open\"}'),
                                                (2,20,'{\"status\":\"closed\"}')",
                )
                .await
                .unwrap();
            reader
                .batch_execute(&format!("BEGIN ISOLATION LEVEL {isolation}; SELECT 1"))
                .await
                .unwrap();
            writer
                .batch_execute(
                    "BEGIN;
                     UPDATE history SET k=30,body='{\"status\":\"archived\"}' WHERE id=1;
                     DELETE FROM history WHERE id=2;
                     INSERT INTO history VALUES (3,5,'{\"status\":\"open\"}');
                     COMMIT",
                )
                .await
                .unwrap();
            for (query, expected) in [
                ("SELECT id FROM history WHERE k=10", vec![1]),
                (
                    "SELECT id FROM history WHERE k>=10 AND k<=20 ORDER BY id",
                    vec![1, 2],
                ),
                (
                    "SELECT id FROM history WHERE k>=0 ORDER BY k LIMIT 1",
                    vec![1],
                ),
                ("SELECT min(k) FROM history", vec![10]),
                ("SELECT max(k) FROM history", vec![20]),
                (
                    "SELECT id FROM history WHERE body @> '{\"status\":\"open\"}'::jsonb",
                    vec![1],
                ),
                ("SELECT id FROM history WHERE id>=0 ORDER BY id", vec![1, 2]),
            ] {
                match reader.query(query, &[]).await {
                    Ok(rows) => {
                        let actual: Vec<i32> = rows.into_iter().map(|row| row.get(0)).collect();
                        if actual != expected {
                            failures.push(format!("{mode:?} {isolation}: {query}: expected {expected:?}, got {actual:?}"));
                        }
                    }
                    Err(error) => failures.push(format!("{mode:?} {isolation}: {query}: {error}")),
                }
            }
            reader.batch_execute("ROLLBACK").await.unwrap();
        }
        drop(reader);
        drop(writer);
        reader_task.abort();
        writer_task.abort();
        server.request_shutdown();
        task.join().unwrap().unwrap();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
