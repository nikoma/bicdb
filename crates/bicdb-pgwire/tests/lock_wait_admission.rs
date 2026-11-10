//! Lock waiters must leave execution capacity for the sessions that unblock them.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, thread, time::Duration};
use tokio_postgres::NoTls;

async fn lock_owner_progress(lock: &str, release: &str) {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(
        root.path(),
        PgWireConfig {
            max_active_queries: 1,
            max_active_reads: 1,
            max_active_writes: 1,
            overload_timeout: Duration::from_millis(700),
            query_timeout: Duration::from_secs(5),
            automatic_paged_checkpoint: false,
            ..Default::default()
        },
    )
    .unwrap();
    let serving = server.clone();
    let serving_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let config = format!(
        "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
        address.port()
    );
    let (first, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    let first_connection = tokio::spawn(async move { connection.await });
    let (second, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    let second_connection = tokio::spawn(async move { connection.await });
    first.batch_execute("CREATE TABLE admission_rows(id INT PRIMARY KEY,n INT); INSERT INTO admission_rows VALUES(1,0)").await.unwrap();
    first
        .batch_execute(
            "CREATE UNIQUE INDEX admission_n ON admission_rows(n); BEGIN; SAVEPOINT before_lock",
        )
        .await
        .unwrap();
    let statement = match lock {
        "row" => "UPDATE admission_rows SET n=n+1 WHERE id=1",
        "transaction_advisory" => "SELECT pg_advisory_xact_lock(17,42)",
        "session_advisory" => "SELECT pg_advisory_lock(17,42)",
        "unique" => "INSERT INTO admission_rows VALUES(2,7)",
        _ => unreachable!(),
    };
    first.batch_execute(statement).await.unwrap();
    second.batch_execute("BEGIN").await.unwrap();
    let statement = if lock == "unique" {
        "INSERT INTO admission_rows VALUES(3,7)"
    } else {
        statement
    };
    let mut waiting = tokio::spawn(async move {
        let result = second.batch_execute(statement).await;
        let commit = second.batch_execute("COMMIT").await;
        drop(second);
        (result, commit)
    });
    // Observe admission of the actual waiter rather than using a timing guess.
    let observed = tokio::time::timeout(Duration::from_secs(2), async {
        while !server
            .connection_snapshots()
            .iter()
            .any(|connection| connection.active_query.is_some())
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    if observed.is_err() && waiting.is_finished() {
        let early = waiting.await;
        let owner_rows = first
            .query("SELECT id,n FROM admission_rows ORDER BY id", &[])
            .await;
        let owner_rows = owner_rows.map(|rows| {
            rows.into_iter()
                .map(|r| (r.get::<_, i32>(0), r.get::<_, i32>(1)))
                .collect::<Vec<_>>()
        });
        let indexes = first
            .query(
                "SELECT indexname,indexdef FROM pg_indexes WHERE tablename='admission_rows'",
                &[],
            )
            .await;
        let indexes = indexes.map(|rows| {
            rows.into_iter()
                .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
                .collect::<Vec<_>>()
        });
        panic!("{lock} waiter completed before its owner released the lock: {early:?}; owner rows={owner_rows:?}; indexes={indexes:?}");
    }
    observed.unwrap();
    assert!(
        !waiting.is_finished(),
        "the second session must wait on the held lock"
    );
    if lock == "unique" {
        // Merely observing the INSERT while it executes can also catch a
        // nonblocking implementation. It must stay pending while the owner
        // retains the unique key, and then reflect the owner's outcome.
        tokio::select! {
            early = &mut waiting => panic!("unique INSERT did not wait for its owner: {early:?}"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
    let released = first.batch_execute(release).await;
    // Clean up even on the failing source: disconnect releases the owner's lock.
    if released.is_ok() && release != "COMMIT" && release != "ROLLBACK" {
        first.batch_execute("COMMIT").await.unwrap();
    }
    drop(first);
    let waited = tokio::time::timeout(Duration::from_secs(7), waiting).await;
    first_connection.await.unwrap().unwrap();
    second_connection.await.unwrap().unwrap();
    let values = if released.is_ok() && lock == "row" {
        let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let task = tokio::spawn(async move { connection.await });
        let value: i32 = client
            .query_one("SELECT n FROM admission_rows WHERE id=1", &[])
            .await
            .unwrap()
            .get(0);
        drop(client);
        task.await.unwrap().unwrap();
        Some(value)
    } else {
        None
    };
    assert_eq!(
        server.stats_snapshot().peak_active_queries,
        1,
        "lock waits must not raise the execution limit"
    );
    server.request_shutdown();
    serving_thread.join().unwrap().unwrap();
    assert!(
        released.is_ok(),
        "{lock} owner must execute {release} while the waiter is admitted: {released:?}"
    );
    let (statement_result, commit_result) =
        waited.expect("waiter must resume after release").unwrap();
    if lock == "unique" && release == "COMMIT" {
        assert_eq!(
            statement_result.unwrap_err().code().map(|code| code.code()),
            Some("23505"),
            "a committed owner's unique key must reject the waiting INSERT"
        );
    } else {
        assert!(
            statement_result.is_ok(),
            "waiter must acquire the released lock: {statement_result:?}"
        );
    }
    assert!(
        commit_result.is_ok(),
        "waiter must commit: {commit_result:?}"
    );
    if let Some(value) = values {
        assert_eq!(value, if release == "COMMIT" { 2 } else { 1 });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_waiter_allows_owner_commit() {
    lock_owner_progress("row", "COMMIT").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_waiter_allows_owner_rollback() {
    lock_owner_progress("row", "ROLLBACK").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_waiter_allows_owner_rollback_to_savepoint() {
    lock_owner_progress("row", "ROLLBACK TO SAVEPOINT before_lock").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn advisory_waiter_allows_owner_commit() {
    lock_owner_progress("transaction_advisory", "COMMIT").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn advisory_waiter_allows_owner_rollback() {
    lock_owner_progress("transaction_advisory", "ROLLBACK").await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn advisory_waiter_allows_session_unlock() {
    lock_owner_progress("session_advisory", "SELECT pg_advisory_unlock(17,42)").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn constraint_waiter_allows_owner_rollback() {
    lock_owner_progress("unique", "ROLLBACK").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn constraint_waiter_rechecks_owner_commit() {
    lock_owner_progress("unique", "COMMIT").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn constraint_waiter_allows_owner_savepoint_rollback() {
    lock_owner_progress("unique", "ROLLBACK TO SAVEPOINT before_lock").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn definitive_unique_error_does_not_wait_for_an_unrelated_reader() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = PgWireServer::open(root.path(), PgWireConfig::default()).unwrap();
    let serving = server.clone();
    let serving_thread =
        thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let config = format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb");
    let mut clients = Vec::new();
    let mut connections = Vec::new();
    for _ in 0..3 {
        let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        clients.push(client);
        connections.push(tokio::spawn(connection));
    }
    let duplicate = clients.pop().unwrap();
    let reader = clients.pop().unwrap();
    let owner = clients.pop().unwrap();
    owner.batch_execute("CREATE TABLE error_rows(id int PRIMARY KEY); INSERT INTO error_rows VALUES(1); SELECT pg_advisory_lock(17,42)").await.unwrap();
    let waiting = tokio::spawn(async move {
        reader
            .batch_execute("SELECT pg_advisory_lock(17,42)")
            .await
            .unwrap();
        drop(reader);
    });
    // Observe an actual advisory wait, rather than a briefly active query.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let count: i64 = duplicate
                .query_one(
                    "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if count == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        duplicate.batch_execute("INSERT INTO error_rows VALUES(1)"),
    )
    .await;
    // Disconnect the advisory owner even on a failing implementation so the
    // unrelated reader and any queued exclusive replay can finish cleanup.
    drop(owner);
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    drop(duplicate);
    for connection in connections {
        tokio::time::timeout(Duration::from_secs(5), connection)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    server.request_shutdown();
    serving_thread.join().unwrap().unwrap();
    assert_eq!(
        result
            .expect("a definitive error must not queue an exclusive replay")
            .unwrap_err()
            .code()
            .map(|code| code.code()),
        Some("23505")
    );
}
