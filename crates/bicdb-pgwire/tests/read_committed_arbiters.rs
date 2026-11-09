//! Remaining READ COMMITTED arbiter paths, kept separate from the ordinary
//! primary-key repair. Each schedule reconciles the committed final rows.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, sync::Arc, thread, time::Duration};
use tokio_postgres::{Client, SimpleQueryMessage};

async fn connect(port: u16) -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}
fn values(messages: Vec<SimpleQueryMessage>) -> Vec<Vec<String>> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("NULL").to_string())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}
struct Case {
    schema: &'static str,
    winner: &'static str,
    upsert: &'static str,
    expected: Vec<Vec<&'static str>>,
    verify: &'static str,
    final_rows: Vec<Vec<&'static str>>,
    waits: bool,
}
async fn schedule(mode: StorageMode, case: Case) {
    let dir = tempfile::tempdir().unwrap();
    drop(
        BicDb::open_with_config(
            dir.path(),
            DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = PgWireServer::open(
        dir.path(),
        PgWireConfig {
            fsync: false,
            ..Default::default()
        },
    )
    .unwrap();
    let serving = server.clone();
    let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let setup = connect(port).await;
    setup.batch_execute(case.schema).await.unwrap();
    let writer = connect(port).await;
    writer.batch_execute("BEGIN").await.unwrap();
    writer.batch_execute(case.winner).await.unwrap();
    let reader = Arc::new(connect(port).await);
    reader
        .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        .await
        .unwrap();
    let querying = reader.clone();
    let mut upsert = tokio::spawn(async move { querying.simple_query(case.upsert).await });
    let messages = if case.waits {
        let early = tokio::time::timeout(Duration::from_millis(200), &mut upsert).await;
        assert!(
            early.is_err(),
            "must wait for the transaction holding its unique winner: {early:?}"
        );
        writer.batch_execute("COMMIT").await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), upsert)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    } else {
        let messages = tokio::time::timeout(Duration::from_secs(5), upsert)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        writer.batch_execute("COMMIT").await.unwrap();
        messages
    };
    assert_eq!(values(messages), case.expected);
    reader.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        values(setup.simple_query(case.verify).await.unwrap()),
        case.final_rows
    );
    server.request_shutdown();
    task.join().unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secondary_unique_pending_winner_is_updated_under_read_committed() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        schedule(mode, Case {
            schema:"CREATE TABLE arbiter_rows(id INT PRIMARY KEY,k TEXT UNIQUE,v INT)",
            winner:"INSERT INTO arbiter_rows VALUES(1,'same',20)",
            upsert:"INSERT INTO arbiter_rows VALUES(2,'same',10) ON CONFLICT(k) DO UPDATE SET v=arbiter_rows.v+EXCLUDED.v RETURNING id,k,v",
            expected:vec![vec!["1","same","30"]], verify:"SELECT id,k,v FROM arbiter_rows ORDER BY id", final_rows:vec![vec!["1","same","30"]], waits:true,
        }).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_primary_pending_winner_is_updated_under_read_committed() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for (schema,winner,upsert) in [
            ("CREATE TABLE arbiter_rows(id CHAR(4) PRIMARY KEY,v INT)","INSERT INTO arbiter_rows VALUES('ab',20)","INSERT INTO arbiter_rows VALUES('ab  ',10) ON CONFLICT(id) DO UPDATE SET v=arbiter_rows.v+EXCLUDED.v RETURNING v"),
            ("CREATE TABLE arbiter_rows(id NUMERIC(10,2) PRIMARY KEY,v INT)","INSERT INTO arbiter_rows VALUES(1.0,20)","INSERT INTO arbiter_rows VALUES(1.00,10) ON CONFLICT(id) DO UPDATE SET v=arbiter_rows.v+EXCLUDED.v RETURNING v"),
        ] {
            schedule(mode.clone(), Case { schema,winner,upsert,expected:vec![vec!["30"]],verify:"SELECT v FROM arbiter_rows",final_rows:vec![vec!["30"]],waits:true }).await;
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_nulls_not_distinct_waits_but_distinct_nulls_do_not() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for (constraint, waits, expected, final_rows) in [
            (
                "UNIQUE NULLS NOT DISTINCT",
                true,
                vec![vec!["1", "30"]],
                vec![vec!["1", "30"]],
            ),
            (
                "UNIQUE",
                false,
                vec![vec!["2", "10"]],
                vec![vec!["1", "20"], vec!["2", "10"]],
            ),
        ] {
            let schema: &'static str = if constraint == "UNIQUE" {
                "CREATE TABLE arbiter_rows(id INT PRIMARY KEY,k TEXT UNIQUE,v INT)"
            } else {
                "CREATE TABLE arbiter_rows(id INT PRIMARY KEY,k TEXT UNIQUE NULLS NOT DISTINCT,v INT)"
            };
            schedule(mode.clone(), Case { schema,winner:"INSERT INTO arbiter_rows VALUES(1,NULL,20)",upsert:"INSERT INTO arbiter_rows VALUES(2,NULL,10) ON CONFLICT(k) DO UPDATE SET v=arbiter_rows.v+EXCLUDED.v RETURNING id,v",expected,verify:"SELECT id,v FROM arbiter_rows ORDER BY id",final_rows,waits }).await;
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secondary_unique_key_vacated_by_waited_delete_or_move_inserts_new_identity() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        for (winner, final_rows) in [
            (
                "DELETE FROM arbiter_rows WHERE id=1",
                vec![vec!["2", "same", "10"]],
            ),
            (
                "UPDATE arbiter_rows SET k='other' WHERE id=1",
                vec![vec!["1", "other", "20"], vec!["2", "same", "10"]],
            ),
        ] {
            schedule(mode.clone(), Case { schema:"CREATE TABLE arbiter_rows(id INT PRIMARY KEY,k TEXT UNIQUE,v INT); INSERT INTO arbiter_rows VALUES(1,'same',20)",winner,upsert:"INSERT INTO arbiter_rows VALUES(2,'same',10) ON CONFLICT(k) DO UPDATE SET v=arbiter_rows.v+EXCLUDED.v RETURNING id,k,v",expected:vec![vec!["2","same","10"]],verify:"SELECT id,k,v FROM arbiter_rows ORDER BY id",final_rows,waits:true }).await;
        }
    }
}
