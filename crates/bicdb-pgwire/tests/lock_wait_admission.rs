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
    let waiting = tokio::spawn(async move {
        let result = second.batch_execute(statement).await;
        let commit = second.batch_execute("COMMIT").await;
        drop(second);
        (result, commit)
    });
    // Observe admission of the actual waiter rather than using a timing guess.
    tokio::time::timeout(Duration::from_secs(2), async {
        while !server
            .connection_snapshots()
            .iter()
            .any(|connection| connection.active_query.as_deref() == Some(statement))
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !waiting.is_finished(),
        "the second session must wait on the held lock"
    );
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
    assert!(
        statement_result.is_ok(),
        "waiter must acquire the released lock: {statement_result:?}"
    );
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
