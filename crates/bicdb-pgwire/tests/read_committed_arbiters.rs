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
    let upsert_sql = case.upsert;
    let mut upsert = tokio::spawn(async move { querying.simple_query(upsert_sql).await });
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

fn secondary() -> Case {
    Case { schema:"CREATE TABLE arbiter_rows(id INT PRIMARY KEY,k TEXT UNIQUE,v INT)",winner:"INSERT INTO arbiter_rows VALUES(1,'same',20)",upsert:"INSERT INTO arbiter_rows VALUES(2,'same',10) ON CONFLICT(k) DO UPDATE SET v=arbiter_rows.v+EXCLUDED.v RETURNING id,k,v",expected:vec![vec!["1","same","30"]],verify:"SELECT id,k,v FROM arbiter_rows ORDER BY id",final_rows:vec![vec!["1","same","30"]],waits:true }
}
fn char_primary() -> Case {
    Case { schema:"CREATE TABLE arbiter_rows(id CHAR(4) PRIMARY KEY,v INT)",winner:"INSERT INTO arbiter_rows VALUES('ab',20)",upsert:"INSERT INTO arbiter_rows VALUES('ab  ',10) ON CONFLICT(id) DO UPDATE SET v=arbiter_rows.v+EXCLUDED.v RETURNING v",expected:vec![vec!["30"]],verify:"SELECT v FROM arbiter_rows",final_rows:vec![vec!["30"]],waits:true }
}
fn numeric_primary() -> Case {
    Case { schema:"CREATE TABLE arbiter_rows(id NUMERIC(10,2) PRIMARY KEY,v INT)",winner:"INSERT INTO arbiter_rows VALUES(1.0,20)",upsert:"INSERT INTO arbiter_rows VALUES(1.00,10) ON CONFLICT(id) DO UPDATE SET v=arbiter_rows.v+EXCLUDED.v RETURNING v",expected:vec![vec!["30"]],verify:"SELECT v FROM arbiter_rows",final_rows:vec![vec!["30"]],waits:true }
}
fn nulls_distinct() -> Case {
    Case { schema:"CREATE TABLE arbiter_rows(id INT PRIMARY KEY,k TEXT UNIQUE,v INT)",winner:"INSERT INTO arbiter_rows VALUES(1,NULL,20)",upsert:"INSERT INTO arbiter_rows VALUES(2,NULL,10) ON CONFLICT(k) DO UPDATE SET v=arbiter_rows.v+EXCLUDED.v RETURNING id,v",expected:vec![vec!["2","10"]],verify:"SELECT id,v FROM arbiter_rows ORDER BY id",final_rows:vec![vec!["1","20"],vec!["2","10"]],waits:false }
}
fn nulls_not_distinct() -> Case {
    Case {
        schema:
            "CREATE TABLE arbiter_rows(id INT PRIMARY KEY,k TEXT UNIQUE NULLS NOT DISTINCT,v INT)",
        expected: vec![vec!["1", "30"]],
        final_rows: vec![vec!["1", "30"]],
        waits: true,
        ..nulls_distinct()
    }
}
fn key_deleted() -> Case {
    Case { schema:"CREATE TABLE arbiter_rows(id INT PRIMARY KEY,k TEXT UNIQUE,v INT); INSERT INTO arbiter_rows VALUES(1,'same',20)",winner:"DELETE FROM arbiter_rows WHERE id=1",expected:vec![vec!["2","same","10"]],final_rows:vec![vec!["2","same","10"]],..secondary() }
}
fn key_moved() -> Case {
    Case {
        winner: "UPDATE arbiter_rows SET k='other' WHERE id=1",
        final_rows: vec![vec!["1", "other", "20"], vec!["2", "same", "10"]],
        ..key_deleted()
    }
}
macro_rules! arbiter_test {
    ($name:ident,$mode:expr,$case:ident) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            schedule($mode, $case()).await;
        }
    };
}
arbiter_test!(secondary_memory, StorageMode::EmbeddedMemory, secondary);
arbiter_test!(secondary_paged, StorageMode::ServerPaged, secondary);
arbiter_test!(
    char_primary_memory,
    StorageMode::EmbeddedMemory,
    char_primary
);
arbiter_test!(char_primary_paged, StorageMode::ServerPaged, char_primary);
arbiter_test!(
    numeric_primary_memory,
    StorageMode::EmbeddedMemory,
    numeric_primary
);
arbiter_test!(
    numeric_primary_paged,
    StorageMode::ServerPaged,
    numeric_primary
);
arbiter_test!(
    nulls_distinct_memory,
    StorageMode::EmbeddedMemory,
    nulls_distinct
);
arbiter_test!(
    nulls_distinct_paged,
    StorageMode::ServerPaged,
    nulls_distinct
);
arbiter_test!(
    nulls_not_distinct_memory,
    StorageMode::EmbeddedMemory,
    nulls_not_distinct
);
arbiter_test!(
    nulls_not_distinct_paged,
    StorageMode::ServerPaged,
    nulls_not_distinct
);
arbiter_test!(key_deleted_memory, StorageMode::EmbeddedMemory, key_deleted);
arbiter_test!(key_deleted_paged, StorageMode::ServerPaged, key_deleted);
arbiter_test!(key_moved_memory, StorageMode::EmbeddedMemory, key_moved);
arbiter_test!(key_moved_paged, StorageMode::ServerPaged, key_moved);
