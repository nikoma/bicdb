use bicdb_pgwire::{LogArchiveConfig, PgWireConfig, PgWireServer, VerificationConfig};
use std::{
    net::TcpListener,
    thread,
    time::{Duration, Instant},
};
use tokio_postgres::SimpleQueryMessage;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_restore_results_are_available_through_the_protocol() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = PgWireConfig::default();
    config.log_archive = Some(LogArchiveConfig {
        dir: root.path().join("archive"),
        base_backup_key: Some("verification-integration-key".into()),
        base_backup_interval: Duration::from_secs(3600),
        keep_base_backups: 2,
    });
    config.verification = Some(VerificationConfig {
        interval: Duration::from_millis(100),
        scratch_root: root.path().join("scratch"),
    });
    let server = PgWireServer::open(root.path().join("db"), config).unwrap();
    let serving = server.clone();
    let handle = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(connection);
    client.batch_execute("CREATE TABLE items (id INT PRIMARY KEY, value TEXT); INSERT INTO items VALUES (1, 'kept'); CREATE INDEX items_value ON items(value)").await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let rows = client
            .simple_query("SELECT * FROM bicdb_verify")
            .await
            .unwrap();
        let completed = rows.iter().any(|message| match message {
            SimpleQueryMessage::Row(row) => {
                row.get("success") == Some("t")
                    && row.get("restore_and_failover") == Some("t")
                    && row
                        .get("indexes")
                        .and_then(|value| value.parse::<usize>().ok())
                        .is_some_and(|indexes| indexes >= 2)
            }
            _ => false,
        });
        if completed {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "scheduled verifier did not publish a successful restore: {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        client
            .query_one("SELECT value FROM items WHERE id = 1", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "kept"
    );
    server.request_shutdown();
    handle.join().unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_verification_reports_legacy_sql_catalog_aliases() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = PgWireConfig::default();
    config.verification = Some(VerificationConfig {
        interval: Duration::from_millis(100),
        scratch_root: root.path().join("scratch"),
    });
    let path = root.path().join("db");
    let shared = std::sync::Arc::new(parking_lot::RwLock::new(
        bicdb_core::BicDb::open(&path).unwrap(),
    ));
    {
        let mut db = shared.write();
        bicdb_sql::SqlSession::new(&mut db).execute("CREATE TABLE original_type(id INT PRIMARY KEY); CREATE TABLE aliased_type(id INT PRIMARY KEY)").unwrap();
        let original = db
            .get("__bicdb_pg_schema", "original_type")
            .unwrap()
            .unwrap();
        let mut aliased = db
            .get("__bicdb_pg_schema", "aliased_type")
            .unwrap()
            .unwrap()
            .as_ref()
            .clone();
        aliased.metadata["row_type_oid"] = original.metadata["row_type_oid"].clone();
        db.insert("__bicdb_pg_schema", aliased).unwrap();
    }
    let server = PgWireServer::open_with_shared(&path, config, shared).unwrap();
    let serving = server.clone();
    let handle = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(connection);
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let rows = client
            .simple_query("SELECT * FROM bicdb_verify")
            .await
            .unwrap();
        if rows.iter().any(|message| match message {
            SimpleQueryMessage::Row(row) => {
                row.get("success") == Some("f")
                    && row
                        .get("error")
                        .is_some_and(|error| error.contains("pg_type") && error.contains("aliases"))
            }
            _ => false,
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "catalog failure was not published: {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    server.request_shutdown();
    handle.join().unwrap().unwrap();
}
