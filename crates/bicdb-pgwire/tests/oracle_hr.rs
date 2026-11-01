//! An Oracle-authored schema over an ordinary PostgreSQL driver. No Oracle
//! server or proprietary driver is involved in this acceptance.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, sync::Arc, thread};

struct Running {
    server: Arc<PgWireServer>,
    thread: Option<thread::JoinHandle<bicdb_pgwire::Result<()>>>,
    port: u16,
}
impl Running {
    fn new(path: &std::path::Path, oracle: bool) -> Self {
        Self::with_profile(path, if oracle { "oracle-hr-v1" } else { "postgres" })
    }
    fn with_profile(path: &std::path::Path, profile: &str) -> Self {
        let config = PgWireConfig {
            sql_profile: profile.into(),
            fsync: false,
            ..Default::default()
        };
        let server = PgWireServer::open(path, config).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let copy = server.clone();
        Self {
            server,
            port,
            thread: Some(thread::spawn(move || {
                bicdb_pgwire::serve_existing_listener(copy, listener)
            })),
        }
    }
    async fn connect(
        &self,
        oracle: bool,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(self.port)
            .user("bicdb")
            .dbname("bicdb");
        if oracle {
            config.options("-c bicdb.sql_profile=oracle-hr-v1");
        }
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
        assert_eq!(
            connection.parameter("bicdb_sql_profile"),
            Some(if oracle { "oracle-hr-v1" } else { "postgres" })
        );
        Ok((
            client,
            tokio::spawn(async move {
                connection.await.unwrap();
            }),
        ))
    }
}

#[tokio::test]
async fn oracle_column_lock_targets_the_owning_join_alias() {
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let connect = || {
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(server.port)
            .user("bicdb")
            .dbname("bicdb")
            .options("-c bicdb.sql_profile=oracle-preview-v1");
        async move {
            let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
            (
                client,
                tokio::spawn(async move {
                    connection.await.unwrap();
                }),
            )
        }
    };
    let (one, c1) = connect().await;
    let (two, c2) = connect().await;
    one.batch_execute(
        "CREATE TABLE parent_rows(k NUMBER PRIMARY KEY, value VARCHAR2(20));
        CREATE TABLE child_rows(k NUMBER PRIMARY KEY, value VARCHAR2(20));
        INSERT INTO parent_rows VALUES(1,'parent'); INSERT INTO child_rows VALUES(1,'child')",
    )
    .await
    .unwrap();
    one.batch_execute("BEGIN").await.unwrap();
    let locked = one
        .query(
            "SELECT p.k FROM parent_rows p JOIN child_rows c ON p.k=c.k FOR UPDATE OF c.value",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(locked.len(), 1);
    // The unrelated parent row remains writable; the selected child is locked.
    two.batch_execute("UPDATE parent_rows SET value='changed' WHERE k=1")
        .await
        .unwrap();
    let error = two
        .batch_execute("UPDATE child_rows SET value='blocked' WHERE k=1")
        .await
        .unwrap_err();
    assert_eq!(
        error.as_db_error().unwrap().code(),
        &tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE
    );
    one.batch_execute("ROLLBACK").await.unwrap();
    two.batch_execute("UPDATE child_rows SET value='released' WHERE k=1")
        .await
        .unwrap();
    assert!(one
        .query(
            "SELECT p.k FROM parent_rows p JOIN child_rows c ON p.k=c.k FOR UPDATE OF value",
            &[]
        )
        .await
        .is_err());
    drop(one);
    drop(two);
    c1.await.unwrap();
    c2.await.unwrap();
}

#[tokio::test]
async fn oracle_preview_schema_selection_applies_to_simple_prepared_and_copy_queries() {
    use futures_util::SinkExt;
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let connect = |path: &str| {
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(server.port)
            .user("bicdb")
            .dbname("bicdb")
            .options(format!(
                "-c bicdb.sql_profile=oracle-preview-v1 -c search_path={path}"
            ));
        async move {
            let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
            (
                client,
                tokio::spawn(async move {
                    connection.await.unwrap();
                }),
            )
        }
    };
    let (admin, connection) = connect("public").await;
    admin
        .batch_execute(
            "CREATE SCHEMA red; CREATE SCHEMA blue;
        CREATE TABLE red.entries(k NUMBER PRIMARY KEY, value VARCHAR2(20));
        CREATE TABLE blue.entries(k NUMBER PRIMARY KEY, value VARCHAR2(20));
        INSERT INTO red.entries VALUES(1,'red'); INSERT INTO blue.entries VALUES(1,'blue')",
        )
        .await
        .unwrap();
    drop(admin);
    connection.await.unwrap();
    for (path, expected) in [("red", "red"), ("\"BLUE\"", "blue")] {
        let (client, connection) = connect(path).await;
        let statement = client
            .prepare_typed(
                "SELECT value FROM entries WHERE k=$1",
                &[tokio_postgres::types::Type::INT4],
            )
            .await
            .unwrap();
        assert_eq!(
            client
                .query_one(&statement, &[&1i32])
                .await
                .unwrap()
                .get::<_, String>(0),
            expected
        );
        let sink = client
            .copy_in("COPY entries(k,value) FROM STDIN")
            .await
            .unwrap();
        tokio::pin!(sink);
        sink.send(&b"2\tlocal\n"[..]).await.unwrap();
        assert_eq!(sink.finish().await.unwrap(), 1);
        let messages = client
            .simple_query("SELECT COUNT(*) FROM entries")
            .await
            .unwrap();
        assert!(messages.iter().any(|message| matches!(message, tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0)==Some("2"))));
        drop(client);
        connection.await.unwrap();
    }
    let (client, connection) = connect("absent").await;
    assert_eq!(
        client
            .query("SELECT * FROM entries", &[])
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .code(),
        &tokio_postgres::error::SqlState::UNDEFINED_TABLE
    );
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn postgres_extended_copy_recovers_after_constraint_error_and_copies_out() {
    use futures_util::{SinkExt, StreamExt};
    let directory = tempfile::tempdir().unwrap();
    let server = Running::new(directory.path(), false);
    let (client, connection) = server.connect(false).await.unwrap();
    client.batch_execute("CREATE TABLE copy_guards(k numeric PRIMARY KEY, value text UNIQUE CHECK(value <> 'bad'))").await.unwrap();
    let sink = client.copy_in("COPY copy_guards FROM STDIN").await.unwrap();
    tokio::pin!(sink);
    sink.send(&b"1\tgood\n"[..]).await.unwrap();
    assert_eq!(sink.finish().await.unwrap(), 1);
    let sink = client.copy_in("COPY copy_guards FROM STDIN").await.unwrap();
    tokio::pin!(sink);
    sink.send(&b"2\tvalid\n3\tbad\n"[..]).await.unwrap();
    assert_eq!(
        sink.finish()
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .code(),
        &tokio_postgres::error::SqlState::CHECK_VIOLATION
    );
    let stream = client.copy_out("COPY copy_guards TO STDOUT").await.unwrap();
    tokio::pin!(stream);
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        data.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(data, b"1\tgood\n");
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM copy_guards", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn oracle_preview_copies_qualified_lobs_and_preserves_constraints_after_restart() {
    use futures_util::SinkExt;
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    assert!(server.connect(false).await.is_err());
    assert!(server.connect(true).await.is_err());
    let connect = |port| async move {
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(port)
            .user("bicdb")
            .dbname("bicdb")
            .options("-c bicdb.sql_profile=oracle-preview-v1")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        assert_eq!(
            connection.parameter("bicdb_sql_profile"),
            Some("oracle-preview-v1")
        );
        (
            client,
            tokio::spawn(async move {
                connection.await.unwrap();
            }),
        )
    };
    let (client, connection) = connect(server.port).await;
    client
        .batch_execute(
            r#"CREATE SCHEMA app;
        CREATE TABLE app.docs(id NUMBER PRIMARY KEY, label VARCHAR2(2 CHAR), payload BLOB,
          document CLOB CHECK(document IS JSON)) NOCACHE"#,
        )
        .await
        .unwrap();
    let sink = client
        .copy_in(r#"COPY app.docs(id,label,payload,document) FROM STDIN"#)
        .await
        .unwrap();
    tokio::pin!(sink);
    sink.send(&b"1\t\xc3\xa9\xc3\xa9\t\\\\x00ff\t{\"ok\":true}\n"[..])
        .await
        .unwrap();
    assert_eq!(sink.finish().await.unwrap(), 1);
    let invalid = client
        .copy_in("COPY app.docs(id,label,payload,document) FROM STDIN")
        .await
        .unwrap();
    tokio::pin!(invalid);
    invalid
        .send(&b"2\tok\t\\N\tinvalid json\n"[..])
        .await
        .unwrap();
    assert_eq!(
        invalid
            .finish()
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .code(),
        &tokio_postgres::error::SqlState::CHECK_VIOLATION
    );
    drop(client);
    connection.await.unwrap();
    drop(server);
    assert!(PgWireServer::open(directory.path(), PgWireConfig::default()).is_err());
    assert!(PgWireServer::open(
        directory.path(),
        PgWireConfig {
            sql_profile: "oracle-hr-v1".into(),
            ..Default::default()
        }
    )
    .is_err());
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let (client, connection) = connect(server.port).await;
    let row = client
        .query_one("SELECT id::text,label,payload,document FROM app.docs", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "1");
    assert_eq!(row.get::<_, String>(1), "éé");
    assert_eq!(row.get::<_, Vec<u8>>(2), vec![0, 255]);
    assert_eq!(row.get::<_, String>(3), r#"{"ok":true}"#);
    drop(client);
    connection.await.unwrap();
}
impl Drop for Running {
    fn drop(&mut self) {
        self.server.request_shutdown();
        self.thread.take().unwrap().join().unwrap().unwrap();
    }
}

#[tokio::test]
async fn published_oracle_hr_runs_over_pg_driver_without_changing_postgres() {
    let directory = tempfile::tempdir().unwrap();
    let server = Running::new(directory.path(), true);
    assert!(
        server.connect(false).await.is_err(),
        "Oracle database requires explicit client opt-in"
    );
    let (client, connection) = server.connect(true).await.unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../../examples/oracle/hr/migration.json")).unwrap();
    for (index, sql) in fixture["statements"].as_array().unwrap().iter().enumerate() {
        client
            .batch_execute(sql.as_str().unwrap())
            .await
            .unwrap_or_else(|e| panic!("HR statement {index}: {e:?}; source: {sql}"));
    }
    let row = client
        .query_one("SELECT COUNT(*)::text FROM employees", &[])
        .await
        .unwrap();
    let employees: String = row.get(0);
    assert_eq!(employees, "107");
    let error = client
        .batch_execute("UPDATE emp_details_view SET salary=salary+1 WHERE employee_id=100")
        .await
        .unwrap_err();
    assert_eq!(
        error.as_db_error().unwrap().code(),
        &tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED
    );
    assert_eq!(
        client
            .query_one("SELECT COUNT(*)::text FROM emp_details_view", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "106"
    );
    assert_eq!(
        client
            .query_one("SELECT current_setting('bicdb.sql_profile')", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "oracle-hr-v1"
    );
    let by_id = client
        .prepare_typed(
            "SELECT first_name FROM employees WHERE employee_id = $1",
            &[tokio_postgres::types::Type::INT4],
        )
        .await
        .unwrap();
    let row = client.query_one(&by_id, &[&100_i32]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "Steven");
    let row = client
        .query_one(
            "SELECT DUMMY, NVL(NULL, 'fallback'), 'A' || NULL FROM DUAL",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "X");
    assert_eq!(row.get::<_, String>(1), "fallback");
    assert_eq!(row.get::<_, String>(2), "A");
    let date = client
        .prepare("SELECT TO_DATE('2020-02-29 12:34:56','YYYY-MM-DD HH24:MI:SS') FROM DUAL")
        .await
        .unwrap();
    assert_eq!(
        date.columns()[0].type_(),
        &tokio_postgres::types::Type::TIMESTAMP
    );
    let view = client
        .prepare("SELECT location_id FROM emp_details_view WHERE employee_id=$1")
        .await
        .unwrap();
    assert_eq!(
        view.columns()[0].type_(),
        &tokio_postgres::types::Type::NUMERIC
    );
    client
        .batch_execute(
            "CREATE TABLE profile_values(id NUMBER PRIMARY KEY, value VARCHAR2(20), happened DATE)",
        )
        .await
        .unwrap();
    let insert = client
        .prepare_typed(
            "INSERT INTO profile_values VALUES ($1, $2, TIMESTAMP '2026-11-01 12:34:56')",
            &[
                tokio_postgres::types::Type::INT4,
                tokio_postgres::types::Type::TEXT,
            ],
        )
        .await
        .unwrap();
    client.execute(&insert, &[&1_i32, &""]).await.unwrap();
    let lookup = client
        .prepare_typed(
            "SELECT value IS NULL, happened::text FROM profile_values WHERE id=$1",
            &[tokio_postgres::types::Type::INT4],
        )
        .await
        .unwrap();
    let row = client.query_one(&lookup, &[&1_i32]).await.unwrap();
    assert!(row.get::<_, bool>(0));
    assert!(row.get::<_, String>(1).contains("12:34:56"));
    let rows=client.query("SELECT department_name, COUNT(employee_id)::text FROM departments d JOIN employees e ON d.department_id=e.department_id GROUP BY department_name ORDER BY department_name",&[]).await.unwrap();
    assert!(!rows.is_empty());
    let row = client
        .query_one("SELECT employees_seq.NEXTVAL::text FROM DUAL", &[])
        .await
        .unwrap();
    assert!(!row.get::<_, String>(0).is_empty());
    client
        .batch_execute(
            "CALL add_job_history(100, DATE '2020-01-01', DATE '2021-01-01', 'AD_PRES', 90)",
        )
        .await
        .unwrap();
    let row = client
        .query_one(
            "SELECT COUNT(*)::text FROM job_history WHERE employee_id=100",
            &[],
        )
        .await
        .unwrap();
    assert!(row.get::<_, String>(0).parse::<usize>().unwrap() > 0);
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute(
            "BEGIN add_job_history(100, DATE '2022-01-01', DATE '2023-01-01', 'AD_PRES', 90); END;",
        )
        .await
        .unwrap();
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT COUNT(*)::text FROM job_history", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "11"
    );
    assert!(client
        .batch_execute("SET bicdb.sql_profile = 'postgres'")
        .await
        .is_err());
    assert!(client
        .query(
            "SELECT set_config('bicdb.sql_profile','postgres',false)",
            &[]
        )
        .await
        .is_err());
    client.batch_execute("RESET ALL").await.unwrap();
    client.batch_execute("DISCARD ALL").await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT current_setting('bicdb.sql_profile')", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "oracle-hr-v1"
    );
    assert!(client
        .batch_execute("CREATE PACKAGE p AS END;")
        .await
        .is_err());
    drop(client);
    connection.await.unwrap();
    drop(server);
    assert!(
        PgWireServer::open(directory.path(), PgWireConfig::default()).is_err(),
        "persisted Oracle profile cannot reopen as PostgreSQL"
    );

    let postgres = tempfile::tempdir().unwrap();
    let server = Running::new(postgres.path(), false);
    assert!(server.connect(true).await.is_err());
    let (client, connection) = server.connect(false).await.unwrap();
    let row = client
        .query_one("SELECT '' IS NULL, 'A' || NULL", &[])
        .await
        .unwrap();
    assert!(!row.get::<_, bool>(0));
    assert_eq!(row.get::<_, Option<String>>(1), None);
    client
        .batch_execute("BEGIN; CREATE TABLE rollback_probe(id int); ROLLBACK;")
        .await
        .unwrap();
    assert!(client
        .query("SELECT * FROM rollback_probe", &[])
        .await
        .is_err());
    drop(client);
    connection.await.unwrap();
}
