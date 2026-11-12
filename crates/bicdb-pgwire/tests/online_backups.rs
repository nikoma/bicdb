use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bicdb_core::{
    BackupRestoreOptions, BicDb, Bm25Parameters, DbConfig, IndexDefinition, IndexField, IndexKind,
    StorageMode,
};
use bicdb_pgwire::{OnlineBackupConfig, PgWireConfig, PgWireServer};
use parking_lot::RwLock;
use tokio_postgres::{Client, SimpleQueryMessage};

const KEY: &str = "live-sql-online-backup-key";

fn db_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_extent_bytes(1024 * 1024)
}

async fn json(client: &Client, sql: &str) -> serde_json::Value {
    let rows = client.simple_query(sql).await.unwrap();
    let text = rows
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => row.get(0),
            _ => None,
        })
        .unwrap();
    serde_json::from_str(text).unwrap()
}

async fn connect(address: std::net::SocketAddr, user: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={user} dbname=bicdb",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(connection);
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_sql_commits_and_ddl_continue_during_a_full_backup_and_restore_exactly() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let repository = root.path().join("repository");
    let mut db = BicDb::open_with_config(&path, db_config()).unwrap();
    {
        let mut sql = bicdb_sql::SqlSession::new(&mut db);
        sql.execute("CREATE TABLE items(id INT PRIMARY KEY, body TEXT); CREATE TABLE gone(id INT PRIMARY KEY)").unwrap();
        for i in 0..80 {
            sql.execute(&format!(
                "INSERT INTO items VALUES({i}, 'snapshot original {}')",
                "pad ".repeat(384)
            ))
            .unwrap();
        }
        sql.execute("CREATE INDEX items_body ON items(body)")
            .unwrap();
    }
    db.create_index(IndexDefinition {
        nulls_not_distinct: false,
        name: "items_fts".into(),
        collection: "items".into(),
        fields: vec![IndexField::MetadataPath(vec!["body".into()])],
        unique: false,
        kind: IndexKind::FullText,
        predicate: None,
        exclusion: None,
    })
    .unwrap();
    db.checkpoint_for_resume().unwrap();
    bicdb_sql::SqlSession::new(&mut db)
        .execute("INSERT INTO items VALUES(999, 'snapshot walonly')")
        .unwrap();
    let db = Arc::new(RwLock::new(db));
    let mut config = PgWireConfig::default();
    config.fsync = false;
    config.automatic_paged_checkpoint = false;
    config.online_backups = Some(OnlineBackupConfig {
        directory: repository.clone(),
        key: KEY.into(),
    });
    config.online_backup_limits.bytes_per_second = 128 * 1024;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open_with_shared(&path, config, Arc::clone(&db)).unwrap();
    let serving = Arc::clone(&server);
    let handle = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let client = connect(address, "bicdb").await;
    client.simple_query("SELECT 1").await.unwrap();
    let expected = db.read().verification_manifest().unwrap();
    let ranking = |db: &BicDb| {
        db.full_text_bm25_top_k(
            "items_fts",
            &["snapshot"],
            Bm25Parameters::default(),
            200,
            true,
        )
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|h| (h.primary_key, h.score.to_bits()))
        .collect::<Vec<_>>()
    };
    let expected_ranking = ranking(&db.read());
    let start = json(&client, "SELECT bicdb_backup_start('full')").await;
    let id = start["id"].as_str().unwrap();
    let status_sql = format!("SELECT bicdb_backup_status('{id}')");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = json(&client, &status_sql).await;
        assert_ne!(status[0]["phase"], "failed", "{status}");
        assert_ne!(
            status[0]["phase"], "complete",
            "copy ended before the SQL concurrency proof"
        );
        if status[0]["progress"]["wal_retention_released"] == true {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "copy did not reach its page phase: {status}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::timeout(Duration::from_secs(3), client.batch_execute(
        "UPDATE items SET body = 'changed after cut' WHERE id < 5; DELETE FROM items WHERE id >= 5 AND id < 10; INSERT INTO items VALUES(1000, 'later'); DROP TABLE gone; CREATE TABLE later(id INT)"
    )).await.expect("ordinary SQL writes were held behind the backup").unwrap();
    // CHECKPOINT is an engine API rather than supported SQL syntax. Exercise
    // its real WAL recycling path after the captured WAL has been copied.
    db.read().checkpoint_for_resume().unwrap();
    let status = json(&client, &status_sql).await;
    assert_eq!(
        status[0]["phase"], "copying",
        "SQL did not finish while the copy was in progress: {status}"
    );
    loop {
        let status = json(&client, &status_sql).await;
        assert_ne!(status[0]["phase"], "failed", "{status}");
        if status[0]["phase"] == "complete" {
            assert_eq!(status[0]["result"]["commit_seq"], expected.commit_seq);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "backup did not complete: {status}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let restored_path = root.path().join("restored");
    bicdb_core::restore_backup(
        repository.join("full.bicbackup"),
        &restored_path,
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: false,
        },
    )
    .unwrap();
    let restored = BicDb::open_with_config(restored_path, db_config()).unwrap();
    assert_eq!(restored.verification_manifest().unwrap(), expected);
    assert_eq!(ranking(&restored), expected_ranking);
    assert!(restored
        .verify_indexes_consistently()
        .unwrap()
        .iter()
        .all(|i| i.valid));
    server.request_shutdown();
    drop(client);
    handle.join().unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_control_rejects_unprivileged_contexts_paths_and_transactions() {
    let root = tempfile::tempdir().unwrap();
    let repository = root.path().join("repository");
    let mut config = PgWireConfig::default();
    config.fsync = false;
    config.online_backups = Some(OnlineBackupConfig {
        directory: repository.clone(),
        key: KEY.into(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open(root.path().join("db"), config).unwrap();
    let serving = Arc::clone(&server);
    let handle = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let admin = connect(address, "bicdb").await;
    admin
        .batch_execute("CREATE ROLE ordinary LOGIN")
        .await
        .unwrap();
    let ordinary = connect(address, "ordinary").await;
    let denied = ordinary
        .simple_query("SELECT bicdb_backup_status()")
        .await
        .unwrap_err();
    assert_eq!(denied.as_db_error().unwrap().code().code(), "42501");
    assert!(ordinary
        .simple_query("SELECT bicdb_backup_start('leaked')")
        .await
        .is_err());
    for sql in [
        "SELECT bicdb_backup_start('../escape')",
        "SELECT bicdb_backup_start('bad') WHERE FALSE",
        "SELECT bicdb_backup_start('bad') FROM pg_class",
    ] {
        assert!(
            admin.simple_query(sql).await.is_err(),
            "unexpectedly accepted {sql}"
        );
    }
    admin.batch_execute("BEGIN").await.unwrap();
    assert!(admin
        .simple_query("SELECT bicdb_backup_start('bad')")
        .await
        .is_err());
    admin.batch_execute("ROLLBACK").await.unwrap();
    admin.batch_execute("SET ROLE ordinary").await.unwrap();
    assert_eq!(
        admin
            .simple_query("SELECT bicdb_backup_status()")
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .code()
            .code(),
        "42501"
    );
    admin.batch_execute("RESET ROLE").await.unwrap();
    admin
        .batch_execute("SET SESSION AUTHORIZATION ordinary")
        .await
        .unwrap();
    assert_eq!(
        admin
            .simple_query("SELECT bicdb_backup_status()")
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .code()
            .code(),
        "42501"
    );
    admin
        .batch_execute("RESET SESSION AUTHORIZATION")
        .await
        .unwrap();
    assert_eq!(
        json(&admin, "SELECT bicdb_backup_status()").await,
        serde_json::json!([])
    );
    assert!(!repository.exists());
    server.request_shutdown();
    drop(admin);
    drop(ordinary);
    handle.join().unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_single_job_admission_and_shutdown_release_a_slow_copy() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let repository = root.path().join("repository");
    let mut db = BicDb::open_with_config(&path, db_config()).unwrap();
    bicdb_sql::SqlSession::new(&mut db)
        .execute("CREATE TABLE items(id INT PRIMARY KEY)")
        .unwrap();
    let db = Arc::new(RwLock::new(db));
    let mut config = PgWireConfig::default();
    config.fsync = false;
    config.automatic_paged_checkpoint = false;
    config.online_backups = Some(OnlineBackupConfig {
        directory: repository.clone(),
        key: KEY.into(),
    });
    config.online_backup_limits.bytes_per_second = 1024;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = PgWireServer::open_with_shared(&path, config, Arc::clone(&db)).unwrap();
    let serving = Arc::clone(&server);
    let handle = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let client = connect(address, "bicdb").await;
    let start = json(&client, "SELECT bicdb_backup_start('cancelled')").await;
    let id = start["id"].as_str().unwrap();
    assert!(client
        .simple_query("SELECT bicdb_backup_start('concurrent')")
        .await
        .is_err());
    let status = format!("SELECT bicdb_backup_status('{id}')");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let progress = json(&client, &status).await;
        assert_ne!(progress[0]["phase"], "failed", "{progress}");
        if progress[0]["progress"]["source_bytes_read"]
            .as_u64()
            .unwrap()
            > 0
        {
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::timeout(
        Duration::from_secs(2),
        client.batch_execute("INSERT INTO items VALUES(1)"),
    )
    .await
    .unwrap()
    .unwrap();
    let cancelled = json(&client, &format!("SELECT bicdb_backup_cancel('{id}')")).await;
    assert_eq!(cancelled["cancellation_requested"], true);
    loop {
        let progress = json(&client, &status).await;
        if progress[0]["phase"] == "cancelled" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cancellation did not release the worker: {progress}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(!repository.join("cancelled.bicbackup").exists());
    json(&client, "SELECT bicdb_backup_start('shutdown')").await;
    server.request_shutdown();
    drop(client);
    tokio::task::spawn_blocking(move || handle.join().unwrap().unwrap())
        .await
        .unwrap();
    assert!(!repository.join("shutdown.bicbackup").exists());
    assert!(db.read().get("items", "1").unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_server_honors_persisted_paged_mode_and_reuses_extents_over_sql() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    let repository = root.path().join("repository");
    let mut db = BicDb::open_with_config(&path, db_config()).unwrap();
    bicdb_sql::SqlSession::new(&mut db)
        .execute("CREATE TABLE items(id INT PRIMARY KEY); INSERT INTO items VALUES(1)")
        .unwrap();
    db.checkpoint_for_resume().unwrap();
    drop(db);
    let mut config = PgWireConfig::default();
    config.fsync = false;
    config.automatic_paged_checkpoint = false;
    config.online_backups = Some(OnlineBackupConfig {
        directory: repository.clone(),
        key: KEY.into(),
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    // This exercises real standalone startup, rather than injecting a shared
    // handle whose correctly configured storage mode could hide a CLI defect.
    let server = PgWireServer::open(&path, config).unwrap();
    let serving = Arc::clone(&server);
    let handle = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let client = connect(address, "bicdb").await;
    let deadline = Instant::now() + Duration::from_secs(30);
    for (name, parent) in [("full", None), ("incremental", Some("full"))] {
        let sql = match parent {
            None => format!("SELECT bicdb_backup_start('{name}')"),
            Some(parent) => format!("SELECT bicdb_backup_start('{name}', '{parent}')"),
        };
        let start = json(&client, &sql).await;
        let id = start["id"].as_str().unwrap();
        loop {
            let status = json(&client, &format!("SELECT bicdb_backup_status('{id}')")).await;
            assert_ne!(status[0]["phase"], "failed", "{status}");
            if status[0]["phase"] == "complete" {
                assert!(status[0]["result"]["wal_lsn"].as_u64().is_some());
                if parent.is_some() {
                    assert!(
                        status[0]["result"]["page_extents_reused"].as_u64().unwrap() > 0,
                        "{status}"
                    );
                }
                break;
            }
            assert!(Instant::now() < deadline, "{status}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    server.request_shutdown();
    drop(client);
    handle.join().unwrap().unwrap();
    drop(server);
    let target = root.path().join("restored");
    bicdb_core::restore_backup(
        repository.join("incremental.bicbackup"),
        &target,
        BackupRestoreOptions {
            passphrase: KEY.into(),
            force: false,
        },
    )
    .unwrap();
    let mut restored = BicDb::open_with_config(target, db_config()).unwrap();
    let result = bicdb_sql::SqlSession::new(&mut restored)
        .execute("SELECT COUNT(*) FROM items")
        .unwrap();
    assert_eq!(result.rows[0][0], bicdb_sql::SqlValue::Int(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_control_survives_oracle_identifier_rewriting_and_postgres_quoting() {
    async fn profile_connect(address: std::net::SocketAddr, user: &str, profile: &str) -> Client {
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(address.port())
            .user(user)
            .dbname("bicdb");
        config.options(&format!("-c bicdb.sql_profile={profile}"));
        let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    }
    for profile in [
        bicdb_sql::oracle::ORACLE_PREVIEW_PROFILE,
        bicdb_sql::oracle::POSTGRES_PROFILE,
    ] {
        let root = tempfile::tempdir().unwrap();
        let repository = root.path().join("repository");
        let mut config = PgWireConfig::default();
        config.sql_profile = profile.into();
        config.fsync = true;
        config.automatic_paged_checkpoint = false;
        config.online_backups = Some(OnlineBackupConfig {
            directory: repository.clone(),
            key: KEY.into(),
        });
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = PgWireServer::open(root.path().join("db"), config).unwrap();
        let serving = Arc::clone(&server);
        let handle =
            thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        let admin = profile_connect(address, "bicdb", profile).await;
        assert_eq!(
            json(&admin, "SELECT bicdb_backup_status()").await,
            serde_json::json!([]),
            "{profile}"
        );
        assert_eq!(
            json(&admin, "SELECT \"bicdb_backup_status\"()").await,
            serde_json::json!([]),
            "{profile}"
        );
        if profile == bicdb_sql::oracle::POSTGRES_PROFILE {
            assert!(admin
                .simple_query("SELECT \"BICDB_BACKUP_STATUS\"()")
                .await
                .is_err());
        } else {
            assert_eq!(
                json(&admin, "SELECT \"BICDB_BACKUP_STATUS\"()").await,
                serde_json::json!([])
            );
        }
        assert!(admin
            .simple_query("SELECT public.bicdb_backup_status()")
            .await
            .is_err());
        // Both dialects must keep ordinary roles and explicit transactions
        // outside this administrative dispatcher, including quoted calls.
        admin
            .batch_execute("CREATE ROLE ordinary LOGIN")
            .await
            .unwrap();
        let ordinary = profile_connect(address, "ordinary", profile).await;
        let denied = ordinary
            .simple_query("SELECT \"bicdb_backup_status\"()")
            .await
            .unwrap_err();
        assert_eq!(
            denied.as_db_error().unwrap().code().code(),
            "42501",
            "{profile}"
        );
        admin.batch_execute("BEGIN").await.unwrap();
        assert!(admin
            .simple_query("SELECT \"bicdb_backup_start\"('in_transaction')")
            .await
            .is_err());
        admin.batch_execute("ROLLBACK").await.unwrap();
        assert!(!repository.exists());
        admin.batch_execute("CREATE TABLE dispatch_items(item_key INT PRIMARY KEY,payload TEXT); INSERT INTO dispatch_items VALUES(1,'acknowledged')").await.unwrap();
        let start = json(&admin, "SELECT bicdb_backup_start('profile_dispatch')").await;
        let id = start["id"].as_str().unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let status = json(&admin, &format!("SELECT \"bicdb_backup_status\"('{id}')")).await;
            assert_ne!(status[0]["phase"], "failed", "{profile}: {status}");
            if status[0]["phase"] == "complete" {
                break;
            }
            assert!(Instant::now() < deadline, "{profile}: {status}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let cancelled = json(&admin, &format!("SELECT \"bicdb_backup_cancel\"('{id}')")).await;
        assert_eq!(cancelled["cancellation_requested"], false);
        let restored_path = root.path().join("restored");
        bicdb_core::restore_backup(
            repository.join("profile_dispatch.bicbackup"),
            &restored_path,
            BackupRestoreOptions {
                passphrase: KEY.into(),
                force: false,
            },
        )
        .unwrap();
        // PgWireServer::open uses the default embedded storage here. Restore
        // must preserve that mode; existing paged backup controls use db_config.
        let mut restored =
            BicDb::open_with_config(restored_path, DbConfig::default().with_fsync(true)).unwrap();
        assert_eq!(
            bicdb_sql::SqlSession::new(&mut restored)
                .execute("SELECT payload FROM dispatch_items WHERE item_key=1")
                .unwrap()
                .rows,
            vec![vec![bicdb_sql::SqlValue::String("acknowledged".into())]]
        );
        restored.close().unwrap();
        server.request_shutdown();
        drop(admin);
        drop(ordinary);
        handle.join().unwrap().unwrap();
    }
}
