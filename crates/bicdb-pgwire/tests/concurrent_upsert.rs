//! Concurrent upserts on one unique key, the write pattern of a
//! database-backed cache (`INSERT ... ON CONFLICT (key_hash) DO UPDATE`) used
//! by several worker processes at once.
//!
//! Two autocommit upserts of a new key can both find no row under their
//! snapshots and both buffer an insert. The second commit then loses the
//! unique-key race. The statement must not fail with that conflict: it must
//! re-run and take the update path, as an upsert never reports a unique
//! violation on its arbiter index.
//!
//! A plain INSERT that loses the same race does violate the constraint. It
//! must fail with SQLSTATE 23505 (`unique_violation`), which drivers map to
//! their "record not unique" error, rather than as an internal error.

use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use tokio::sync::Barrier;
use tokio_postgres::{Client, SimpleQueryMessage};

const WORKERS: usize = 8;
const HOT_KEYS: i64 = 4;
const ROUNDS: usize = 60;

struct Running {
    server: Arc<PgWireServer>,
    thread: thread::JoinHandle<bicdb_pgwire::Result<()>>,
    port: u16,
}

fn start(path: &std::path::Path) -> Running {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut config = PgWireConfig::default();
    config.fsync = false;
    let server = PgWireServer::open(path, config).unwrap();
    let serving = Arc::clone(&server);
    let thread = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    Running {
        server,
        thread,
        port,
    }
}

fn stop(running: Running) {
    running.server.request_shutdown();
    running.thread.join().unwrap().unwrap();
}

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

async fn rows(client: &Client, sql: &str) -> Vec<Vec<String>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap_or("NULL").to_string())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

fn database(mode: StorageMode) -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    let db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(mode),
    )
    .unwrap();
    drop(db);
    root
}

const SCHEMA: &str = "CREATE TABLE cache_entries (\
    id BIGSERIAL PRIMARY KEY, \
    key BYTEA NOT NULL, \
    value BYTEA NOT NULL, \
    key_hash BIGINT NOT NULL, \
    byte_size INTEGER NOT NULL, \
    created_at TIMESTAMP NOT NULL); \
    CREATE UNIQUE INDEX cache_entries_key_hash ON cache_entries (key_hash)";

/// Every unique key resolves to exactly one row, through the index and
/// through a scan that cannot use it, and the table holds nothing else.
async fn assert_one_row_per_key(client: &Client, keys: i64) {
    assert_eq!(
        rows(
            client,
            "SELECT count(*), count(DISTINCT key_hash) FROM cache_entries"
        )
        .await,
        vec![vec![keys.to_string(), keys.to_string()]]
    );
    for key in 0..keys {
        let indexed = rows(
            client,
            &format!("SELECT count(*) FROM cache_entries WHERE key_hash = {key}"),
        )
        .await;
        let scanned = rows(
            client,
            &format!("SELECT count(*) FROM cache_entries WHERE key_hash + 0 = {key}"),
        )
        .await;
        assert_eq!(indexed, vec![vec!["1".to_string()]], "index lookup {key}");
        assert_eq!(scanned, vec![vec!["1".to_string()]], "heap scan {key}");
    }
}

async fn concurrent_upserts_never_report_a_unique_violation(mode: StorageMode) {
    let label = format!("{mode:?}");
    let root = database(mode);
    let running = start(root.path());
    let setup = connect(running.port).await;
    setup.batch_execute(SCHEMA).await.unwrap();

    let barrier = Arc::new(Barrier::new(WORKERS));
    let mut workers = Vec::new();
    for worker in 0..WORKERS {
        let client = connect(running.port).await;
        let barrier = Arc::clone(&barrier);
        workers.push(tokio::spawn(async move {
            let mut failures = Vec::new();
            let mut serialization_retries = 0usize;
            for round in 0..ROUNDS {
                // Line every worker up on the same fresh key so the inserts
                // race, then spread the rest over hot keys that already exist.
                barrier.wait().await;
                let key = if round % 2 == 0 {
                    1_000 + round as i64
                } else {
                    (round as i64 + worker as i64) % HOT_KEYS
                };
                let sql = format!(
                    "INSERT INTO cache_entries (key, value, key_hash, byte_size, created_at) \
                     VALUES ('\\x6b', '\\x{worker:02x}{round:04x}', {key}, 3, CURRENT_TIMESTAMP) \
                     ON CONFLICT (key_hash) DO UPDATE SET key = excluded.key, \
                     value = excluded.value, byte_size = excluded.byte_size RETURNING id"
                );
                // A serialization failure (40001) on a contended row is the
                // documented retryable outcome of a lost row-lock race, not
                // of the unique-key race this test is about; retry it as an
                // application would.
                loop {
                    match client.simple_query(&sql).await {
                        Ok(messages) => {
                            let returned = messages
                                .iter()
                                .filter(|message| matches!(message, SimpleQueryMessage::Row(_)))
                                .count();
                            if returned != 1 {
                                failures.push(format!("key {key}: RETURNING gave {returned} rows"));
                            }
                        }
                        Err(error)
                            if error.code()
                                == Some(
                                    &tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE,
                                ) =>
                        {
                            serialization_retries += 1;
                            continue;
                        }
                        Err(error) => failures.push(format!(
                            "key {key}: {:?} {error}",
                            error.code().map(|code| code.code().to_string())
                        )),
                    }
                    break;
                }
            }
            (failures, serialization_retries)
        }));
    }
    let mut failures = Vec::new();
    let mut serialization_retries = 0;
    for worker in workers {
        let (worker_failures, retries) = worker.await.unwrap();
        failures.extend(worker_failures);
        serialization_retries += retries;
    }
    eprintln!("{label}: {serialization_retries} serialization retries");
    assert!(
        failures.is_empty(),
        "{label}: {} upserts failed, first: {:?}",
        failures.len(),
        failures.iter().take(5).collect::<Vec<_>>()
    );
    let fresh_keys = (0..ROUNDS).step_by(2).count() as i64;
    let hot = rows(
        &setup,
        &format!("SELECT count(*) FROM cache_entries WHERE key_hash < {HOT_KEYS}"),
    )
    .await;
    assert_eq!(hot, vec![vec![HOT_KEYS.to_string()]]);
    let total = rows(&setup, "SELECT count(*) FROM cache_entries").await;
    assert_eq!(total, vec![vec![(HOT_KEYS + fresh_keys).to_string()]]);
    drop(setup);
    stop(running);

    // The unique index must still agree with the rows after a restart.
    let running = start(root.path());
    let client = connect(running.port).await;
    let keys = rows(&client, "SELECT key_hash FROM cache_entries").await;
    assert_eq!(keys.len() as i64, HOT_KEYS + fresh_keys);
    let distinct = rows(
        &client,
        "SELECT count(DISTINCT key_hash) FROM cache_entries",
    )
    .await;
    assert_eq!(distinct, vec![vec![keys.len().to_string()]]);
    for key in (0..HOT_KEYS).chain((0..ROUNDS).step_by(2).map(|round| 1_000 + round as i64)) {
        let indexed = rows(
            &client,
            &format!("SELECT count(*) FROM cache_entries WHERE key_hash = {key}"),
        )
        .await;
        assert_eq!(indexed, vec![vec!["1".to_string()]], "{label} key {key}");
    }
    drop(client);
    stop(running);
}

async fn racing_plain_inserts_fail_with_unique_violation(mode: StorageMode) {
    let label = format!("{mode:?}");
    let root = database(mode);
    let running = start(root.path());
    let setup = connect(running.port).await;
    setup.batch_execute(SCHEMA).await.unwrap();

    let barrier = Arc::new(Barrier::new(WORKERS));
    let mut workers = Vec::new();
    for worker in 0..WORKERS {
        let client = connect(running.port).await;
        let barrier = Arc::clone(&barrier);
        workers.push(tokio::spawn(async move {
            let mut outcomes = Vec::new();
            for round in 0..ROUNDS as i64 {
                barrier.wait().await;
                let sql = format!(
                    "INSERT INTO cache_entries (key, value, key_hash, byte_size, created_at) \
                     VALUES ('\\x6b', '\\x{worker:02x}', {round}, 1, CURRENT_TIMESTAMP)"
                );
                outcomes.push(match client.simple_query(&sql).await {
                    Ok(_) => (round, None),
                    Err(error) => (
                        round,
                        Some(
                            error
                                .code()
                                .map(|code| code.code().to_string())
                                .unwrap_or_else(|| error.to_string()),
                        ),
                    ),
                });
            }
            outcomes
        }));
    }
    let mut winners = vec![0usize; ROUNDS];
    let mut wrong_codes = Vec::new();
    for worker in workers {
        for (round, outcome) in worker.await.unwrap() {
            match outcome {
                None => winners[round as usize] += 1,
                Some(code) if code == "23505" => {}
                Some(code) => wrong_codes.push(code),
            }
        }
    }
    assert!(
        wrong_codes.is_empty(),
        "{label}: losing inserts must report unique_violation, got {:?}",
        wrong_codes.iter().take(5).collect::<Vec<_>>()
    );
    assert!(
        winners.iter().all(|count| *count == 1),
        "{label}: exactly one insert per key must win: {winners:?}"
    );
    assert_one_row_per_key(&setup, ROUNDS as i64).await;
    drop(setup);
    stop(running);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_upserts_on_one_key_never_fail_in_memory() {
    concurrent_upserts_never_report_a_unique_violation(StorageMode::EmbeddedMemory).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_upserts_on_one_key_never_fail_paged() {
    concurrent_upserts_never_report_a_unique_violation(StorageMode::ServerPaged).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn racing_plain_inserts_report_unique_violation_in_memory() {
    racing_plain_inserts_fail_with_unique_violation(StorageMode::EmbeddedMemory).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn racing_plain_inserts_report_unique_violation_paged() {
    racing_plain_inserts_fail_with_unique_violation(StorageMode::ServerPaged).await;
}
