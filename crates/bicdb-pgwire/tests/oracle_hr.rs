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
async fn nullable_unique_constraints_keep_profile_rules_across_copy_and_restart() {
    use futures_util::{SinkExt, StreamExt};
    for profile in ["postgres", "oracle-preview-v1"] {
        let directory = tempfile::tempdir().unwrap();
        for attempt in 0..2 {
            let server = Running::with_profile(directory.path(), profile);
            let mut config = tokio_postgres::Config::new();
            config
                .host("127.0.0.1")
                .port(server.port)
                .user("bicdb")
                .dbname("bicdb");
            if profile != "postgres" {
                config.options("-c bicdb.sql_profile=oracle-preview-v1");
            }
            let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
            let connection = tokio::spawn(async move {
                connection.await.unwrap();
            });
            if attempt == 0 {
                client.batch_execute("CREATE TABLE nullable_keys(k INT PRIMARY KEY,a NUMERIC(10,2),b VARCHAR(20),CONSTRAINT nullable_key UNIQUE(a,b)); INSERT INTO nullable_keys VALUES(1,NULL,NULL),(2,NULL,NULL),(3,1,NULL)").await.unwrap();
                let duplicate = client
                    .batch_execute("INSERT INTO nullable_keys VALUES(4,1.00,NULL)")
                    .await;
                if profile == "postgres" {
                    duplicate.unwrap();
                } else {
                    assert_eq!(
                        duplicate.unwrap_err().as_db_error().unwrap().code(),
                        &tokio_postgres::error::SqlState::UNIQUE_VIOLATION
                    );
                }
            }
            if attempt == 0 {
                client.batch_execute(r#"CREATE TABLE "collision_guard"(a INT,b INT); CREATE UNIQUE INDEX "CollisionGuard" ON "collision_guard"(a,b)"#).await.unwrap();
                assert!(client.batch_execute(r#"ALTER TABLE "collision_guard" ADD CONSTRAINT "CollisionGuard" UNIQUE NULLS NOT DISTINCT(a,b)"#).await.is_err());
                assert_eq!(client.query_one(r#"SELECT COUNT(*) FROM "pg_catalog"."pg_constraint" WHERE "conname"='CollisionGuard'"#, &[]).await.unwrap().get::<_,i64>(0),0);
                client
                    .batch_execute(
                        r#"CREATE TABLE "MiXeD"("Key" INT PRIMARY KEY,"Value" VARCHAR(10))"#,
                    )
                    .await
                    .unwrap();
                let sink = client.copy_in(r#"COPY "MiXeD" FROM STDIN"#).await.unwrap();
                tokio::pin!(sink);
                sink.send(b"1\thello\n".as_slice()).await.unwrap();
                assert_eq!(sink.finish().await.unwrap(), 1);
                let stream = client
                    .copy_out(r#"COPY "MiXeD"("Value") TO STDOUT"#)
                    .await
                    .unwrap();
                tokio::pin!(stream);
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.next().await {
                    bytes.extend_from_slice(&chunk.unwrap());
                }
                assert_eq!(bytes, b"hello\n");
            }
            if attempt == 0 {
                client.batch_execute(r#"CREATE SCHEMA "ConstraintOwner"; CREATE TABLE "ConstraintOwner"."keys"(k INT NOT NULL,a INT,b INT,CONSTRAINT "owned_key" UNIQUE(a,b),CONSTRAINT "positive_key" CHECK(k>0))"#).await.unwrap();
                let rows = client.query(r#"SELECT n."nspname",c."connamespace"=r."relnamespace" FROM "pg_catalog"."pg_constraint" c JOIN "pg_catalog"."pg_class" r ON r."oid"=c."conrelid" JOIN "pg_catalog"."pg_namespace" n ON n."oid"=c."connamespace" WHERE r."relname"='keys'"#, &[]).await.unwrap();
                assert_eq!(rows.len(), 3);
                for row in rows {
                    assert_eq!(row.get::<_, String>(0), "ConstraintOwner");
                    assert!(row.get::<_, bool>(1));
                }
            }
            let all_null_id = 10 + attempt;
            client
                .execute(
                    "INSERT INTO nullable_keys(k,a,b) VALUES($1,NULL,NULL)",
                    &[&all_null_id],
                )
                .await
                .unwrap();
            if profile != "postgres" {
                let sink = client
                    .copy_in("COPY nullable_keys FROM STDIN")
                    .await
                    .unwrap();
                tokio::pin!(sink);
                sink.send(b"20\t2\tnew\n21\t1.00\t\\N\n".as_slice())
                    .await
                    .unwrap();
                let error = sink.finish().await.unwrap_err();
                assert_eq!(
                    error.as_db_error().unwrap().code(),
                    &tokio_postgres::error::SqlState::UNIQUE_VIOLATION
                );
                let rows = client
                    .query_one("SELECT COUNT(*) FROM nullable_keys WHERE k=20", &[])
                    .await
                    .unwrap();
                assert_eq!(
                    rows.get::<_, i64>(0),
                    0,
                    "COPY must roll back its earlier row"
                );
                let duplicate = client
                    .batch_execute("UPDATE nullable_keys SET a=1 WHERE k=10")
                    .await
                    .unwrap_err();
                assert_eq!(
                    duplicate.as_db_error().unwrap().code(),
                    &tokio_postgres::error::SqlState::UNIQUE_VIOLATION
                );
            }
            let row = client
                .query_one(
                    "SELECT COUNT(*) FROM nullable_keys WHERE a IS NULL AND b IS NULL",
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(row.get::<_, i64>(0), 3 + i64::from(attempt));
            drop(client);
            connection.await.unwrap();
        }
    }
}

#[tokio::test]
async fn preparing_a_typed_projection_with_from_does_not_evaluate_rows() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let directory = tempfile::tempdir().unwrap();
        let server = Running::with_profile(directory.path(), profile);
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(server.port)
            .user("bicdb")
            .dbname("bicdb");
        if profile != "postgres" {
            config.options("-c bicdb.sql_profile=oracle-preview-v1");
        }
        let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let connection = tokio::spawn(async move {
            connection.await.unwrap();
        });
        client.batch_execute("CREATE TABLE source_rows(k INT PRIMARY KEY); INSERT INTO source_rows VALUES(1),(2)").await.unwrap();
        let statement = client
            .prepare("SELECT 1/(k-k) AS deferred_error FROM source_rows")
            .await
            .unwrap();
        let error = client.query(&statement, &[]).await.unwrap_err();
        assert_eq!(
            error.as_db_error().unwrap().code(),
            &tokio_postgres::error::SqlState::DIVISION_BY_ZERO
        );
        assert_eq!(
            client
                .query_one("SELECT COUNT(*) FROM source_rows", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            2
        );
        drop(client);
        connection.await.unwrap();
    }
}

#[tokio::test]
async fn first_read_committed_row_lock_waits_for_a_younger_holder() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let directory = tempfile::tempdir().unwrap();
        let server = Running::with_profile(directory.path(), profile);
        let connect = || {
            let mut config = tokio_postgres::Config::new();
            config
                .host("127.0.0.1")
                .port(server.port)
                .user("bicdb")
                .dbname("bicdb");
            if profile != "postgres" {
                config.options("-c bicdb.sql_profile=oracle-preview-v1");
            }
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
        let (older, c1) = connect().await;
        let (younger, c2) = connect().await;
        older
            .batch_execute(
                "CREATE TABLE counters(k INT PRIMARY KEY,n INT); INSERT INTO counters VALUES(1,1)",
            )
            .await
            .unwrap();
        older.batch_execute("BEGIN").await.unwrap();
        younger.batch_execute("BEGIN").await.unwrap();
        younger
            .query_one("SELECT n FROM counters WHERE k=1 FOR UPDATE", &[])
            .await
            .unwrap();
        let mut waiting = tokio::spawn(async move {
            let value = older
                .query_one("SELECT n FROM counters WHERE k=1 FOR UPDATE", &[])
                .await;
            if value.is_ok() {
                older
                    .batch_execute("UPDATE counters SET n=n+1 WHERE k=1; COMMIT")
                    .await
                    .unwrap();
            } else {
                older.batch_execute("ROLLBACK").await.unwrap();
            }
            value.map(|row| row.get::<_, i32>(0))
        });
        let early = tokio::time::timeout(std::time::Duration::from_millis(150), &mut waiting).await;
        younger
            .batch_execute("UPDATE counters SET n=2 WHERE k=1; COMMIT")
            .await
            .unwrap();
        let (waited, value) = match early {
            Ok(value) => (false, value.unwrap()),
            Err(_) => (true, waiting.await.unwrap()),
        };
        assert!(
            waited,
            "{profile}: an older first-row waiter should wait for its holder"
        );
        assert_eq!(
            value.unwrap(),
            2,
            "{profile}: locking read must refresh the committed row"
        );
        assert_eq!(
            younger
                .query_one("SELECT n FROM counters WHERE k=1", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            3
        );
        drop(younger);
        c1.await.unwrap();
        c2.await.unwrap();
    }
}

#[tokio::test]
async fn oracle_preview_stored_functions_execute_through_pg_driver_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    for attempt in 0..2 {
        let server = Running::with_profile(directory.path(), "oracle-preview-v1");
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(server.port)
            .user("bicdb")
            .dbname("bicdb")
            .options("-c bicdb.sql_profile=oracle-preview-v1")
            .connect(tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection = tokio::spawn(async move {
            connection.await.unwrap();
        });
        if attempt == 0 {
            for source in [
                "CREATE OR REPLACE FUNCTION constant_time RETURN DATE AS BEGIN RETURN TIMESTAMP '2024-02-29 23:59:58'; END;",
                "CREATE OR REPLACE FUNCTION initial_letter(p_value IN VARCHAR2, p_pos IN NUMBER) RETURN VARCHAR2 IS BEGIN RETURN SUBSTR(p_value,p_pos,1); END initial_letter;",
            ] {
                client.batch_execute(source).await.unwrap();
            }
        }
        let statement = client
            .prepare("SELECT constant_time(),initial_letter('Example',2) FROM DUAL")
            .await
            .unwrap();
        assert_eq!(
            *statement.columns()[0].type_(),
            tokio_postgres::types::Type::TIMESTAMP
        );
        let row = client.query_one(&statement, &[]).await.unwrap();
        assert_eq!(
            row.get::<_, std::time::SystemTime>(0),
            std::time::UNIX_EPOCH + std::time::Duration::from_secs(1709251198)
        );
        assert_eq!(row.get::<_, String>(1), "x");
        drop(client);
        connection.await.unwrap();
    }
}

#[tokio::test]
async fn oracle_number_division_has_numeric_metadata_and_recovers_after_zero() {
    use tokio_postgres::types::Type;
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(server.port)
        .user("bicdb")
        .dbname("bicdb")
        .options("-c bicdb.sql_profile=oracle-preview-v1")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(async move {
        connection.await.unwrap();
    });
    let typed = client
        .prepare_typed("SELECT $1/$2", &[Type::INT4, Type::INT4])
        .await
        .unwrap();
    assert_eq!(typed.params(), &[Type::INT4, Type::INT4]);
    assert_eq!(typed.columns()[0].type_(), &Type::NUMERIC);
    client.query_one(&typed, &[&7i32, &2i32]).await.unwrap();
    let inferred = client.prepare("SELECT $1/$2").await.unwrap();
    assert_eq!(inferred.params(), &[Type::NUMERIC, Type::NUMERIC]);
    let row = client
        .query_one("SELECT CASE WHEN 7/2>3 THEN 1 ELSE 0 END FROM DUAL", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);
    let zero = client.prepare("SELECT 1/0 FROM DUAL").await.unwrap();
    assert_eq!(zero.columns()[0].type_(), &Type::NUMERIC);
    assert_eq!(
        client
            .query(&zero, &[])
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .code(),
        &tokio_postgres::error::SqlState::DIVISION_BY_ZERO
    );
    let rows = client.simple_query("SELECT 7/2 FROM DUAL").await.unwrap();
    assert!(rows.iter().any(|message| matches!(message,tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0).is_some_and(|value|value.trim_end_matches('0').trim_end_matches('.')=="3.5"))));
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn oracle_day_offsets_keep_typed_parameters_and_timestamp_results() {
    use tokio_postgres::types::Type;
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(server.port)
        .user("bicdb")
        .dbname("bicdb")
        .options("-c bicdb.sql_profile=oracle-preview-v1")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(async move {
        connection.await.unwrap();
    });
    let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(946_684_800);
    let fractional = start + std::time::Duration::from_millis(900);
    for sql in ["SELECT $1+$2", "SELECT $2+$1"] {
        let statement = client
            .prepare_typed(sql, &[Type::TIMESTAMP, Type::INT4])
            .await
            .unwrap();
        assert_eq!(statement.columns()[0].type_(), &Type::TIMESTAMP);
        assert_eq!(
            client
                .query_one(&statement, &[&fractional, &1i32])
                .await
                .unwrap()
                .get::<_, std::time::SystemTime>(0),
            start + std::time::Duration::from_secs(86400)
        );
    }
    let statement = client
        .prepare_typed("SELECT $1-$2", &[Type::TIMESTAMP, Type::TIMESTAMP])
        .await
        .unwrap();
    assert_eq!(statement.columns()[0].type_(), &Type::INTERVAL);
    let statement = client
        .prepare("SELECT TIMESTAMP '2000-01-01 00:00:00'+$1")
        .await
        .unwrap();
    assert_eq!(statement.params(), &[Type::NUMERIC]);
    assert_eq!(statement.columns()[0].type_(), &Type::TIMESTAMP);
    client.batch_execute("CREATE SCHEMA offset_app; CREATE TABLE offset_app.events(k NUMBER PRIMARY KEY,happened DATE); INSERT INTO offset_app.events VALUES(1,TIMESTAMP '2000-01-01 00:00:00')").await.unwrap();
    let statement = client
        .prepare_typed(
            "SELECT happened+$1 FROM offset_app.events WHERE k=1",
            &[Type::INT4],
        )
        .await
        .unwrap();
    assert_eq!(statement.columns()[0].type_(), &Type::TIMESTAMP);
    assert_eq!(
        client
            .query_one(&statement, &[&1i32])
            .await
            .unwrap()
            .get::<_, std::time::SystemTime>(0),
        start + std::time::Duration::from_secs(86400)
    );
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn oracle_datetime_trunc_preserves_prepared_timestamp_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(server.port)
        .user("bicdb")
        .dbname("bicdb")
        .options("-c bicdb.sql_profile=oracle-preview-v1")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(async move {
        connection.await.unwrap();
    });
    let statement = client
        .prepare_typed(
            "SELECT TRUNC($1,$2)",
            &[
                tokio_postgres::types::Type::TIMESTAMP,
                tokio_postgres::types::Type::TEXT,
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        *statement.columns()[0].type_(),
        tokio_postgres::types::Type::TIMESTAMP
    );
    let input = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1709251198);
    let expected = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1706745600);
    assert_eq!(
        client
            .query_one(&statement, &[&input, &"MM"])
            .await
            .unwrap()
            .get::<_, std::time::SystemTime>(0),
        expected
    );
    assert!(client
        .query_one(&statement, &[&input, &Option::<String>::None])
        .await
        .unwrap()
        .get::<_, Option<std::time::SystemTime>>(0)
        .is_none());
    drop(client);
    connection.await.unwrap();
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
        CREATE TABLE red.entries(k NUMBER PRIMARY KEY, value VARCHAR2(20), entries VARCHAR2(20));
        CREATE TABLE blue.entries(k NUMBER PRIMARY KEY, value VARCHAR2(20), entries VARCHAR2(20));
        INSERT INTO red.entries VALUES(1,'red','red'); INSERT INTO blue.entries VALUES(1,'blue','blue')",
        )
        .await
        .unwrap();
    drop(admin);
    connection.await.unwrap();
    for (path, expected) in [("red", "red"), ("\"BLUE\"", "blue")] {
        let (client, connection) = connect(path).await;
        let statement = client
            .prepare_typed(
                "SELECT entries.value FROM entries WHERE entries.k=$1",
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
        assert_eq!(
            client
                .query_one("SELECT entries FROM entries WHERE k=1", &[])
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

    let (client, connection) = connect("red").await;
    client
        .batch_execute(
            "CREATE INDEX local_drop_idx ON entries(value);
        CREATE SEQUENCE local_drop_seq;
        CREATE VIEW local_drop_view AS SELECT k FROM entries;
        DROP INDEX local_drop_idx;
        DROP SEQUENCE local_drop_seq;
        DROP VIEW local_drop_view;
        DROP TABLE entries",
        )
        .await
        .unwrap();
    assert!(client
        .simple_query("SELECT * FROM red.entries")
        .await
        .is_err());
    let rows = client
        .simple_query("SELECT COUNT(*) FROM blue.entries")
        .await
        .unwrap();
    assert!(rows.iter().any(
        |m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0)==Some("2"))
    ));
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

#[tokio::test]
async fn nullable_composite_unique_policy_is_enforced_over_the_pg_driver() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let root = tempfile::tempdir().unwrap();
        let running = Running::with_profile(root.path(), profile);
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(running.port)
            .user("bicdb")
            .dbname("bicdb")
            .options(&format!("-c bicdb.sql_profile={profile}"));
        let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let task = tokio::spawn(async move { connection.await.unwrap() });
        client.batch_execute("CREATE TABLE unique_policy(k INTEGER PRIMARY KEY,a INTEGER,b VARCHAR(20));
            CREATE UNIQUE INDEX unique_policy_uq ON unique_policy(a,b) NULLS NOT DISTINCT WHERE a IS NOT NULL OR b IS NOT NULL;
            INSERT INTO unique_policy VALUES(1,NULL,NULL),(2,NULL,NULL)").await.unwrap();
        let insert = client
            .prepare("INSERT INTO unique_policy VALUES($1,$2,$3)")
            .await
            .unwrap();
        let absent: Option<&str> = None;
        client
            .execute(&insert, &[&3i32, &1i32, &absent])
            .await
            .unwrap();
        let error = client
            .execute(&insert, &[&4i32, &1i32, &absent])
            .await
            .unwrap_err();
        assert_eq!(error.code().unwrap().code(), "23505");
        client
            .execute(&insert, &[&4i32, &1i32, &Some("NULL")])
            .await
            .unwrap();
        assert_eq!(
            client
                .query_one("SELECT COUNT(*) FROM unique_policy", &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            4
        );
        client
            .batch_execute(
                "CREATE TABLE ordinary_policy(k INTEGER PRIMARY KEY,a INTEGER,b VARCHAR(20));
            CREATE UNIQUE INDEX ordinary_policy_uq ON ordinary_policy(a,b);
            INSERT INTO ordinary_policy VALUES(1,1,NULL),(2,1,NULL)",
            )
            .await
            .unwrap();
        client.batch_execute("CREATE TABLE source_keys(k INTEGER PRIMARY KEY,tenant NUMERIC,role_id NUMERIC,language VARCHAR(6));
            CREATE UNIQUE INDEX source_keys_uq ON source_keys(tenant,COALESCE(role_id,0),COALESCE(language,'')) NULLS NOT DISTINCT;
            INSERT INTO source_keys VALUES(1,7,NULL,NULL)").await.unwrap();
        let error = client
            .batch_execute("INSERT INTO source_keys VALUES(2,7,0,'')")
            .await
            .unwrap_err();
        assert_eq!(error.code().unwrap().code(), "23505");
        if profile == "postgres" {
            client.batch_execute("CREATE TABLE numeric_nulls(k INTEGER PRIMARY KEY,a NUMERIC,b NUMERIC,note VARCHAR(20));
                CREATE UNIQUE INDEX numeric_nulls_uq ON numeric_nulls(a,b) NULLS NOT DISTINCT;
                INSERT INTO numeric_nulls VALUES(1,1,NULL,'seed');
                INSERT INTO numeric_nulls VALUES(2,1,NULL,'changed') ON CONFLICT(a,b) DO UPDATE SET note=excluded.note").await.unwrap();
            assert_eq!(
                client
                    .query_one("SELECT note FROM numeric_nulls WHERE k=1", &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "changed"
            );
        }
        drop(insert);
        drop(client);
        task.await.unwrap();
        drop(running);
        let running = Running::with_profile(root.path(), profile);
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(running.port)
            .user("bicdb")
            .dbname("bicdb")
            .options(&format!("-c bicdb.sql_profile={profile}"));
        let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let task = tokio::spawn(async move { connection.await.unwrap() });
        let error = client
            .batch_execute("INSERT INTO unique_policy VALUES(5,1,NULL)")
            .await
            .unwrap_err();
        assert_eq!(error.code().unwrap().code(), "23505");
        drop(client);
        task.await.unwrap();
    }
}
