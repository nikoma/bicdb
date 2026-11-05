//! An Oracle-authored schema over an ordinary PostgreSQL driver. No Oracle
//! server or proprietary driver is involved in this acceptance.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, sync::Arc, thread};

struct Running {
    server: Arc<PgWireServer>,
    thread: Option<thread::JoinHandle<bicdb_pgwire::Result<()>>>,
    port: u16,
}

async fn transaction_defaults_connect(
    server: &Running,
    profile: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
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
    (
        client,
        tokio::spawn(async move {
            connection.await.unwrap();
        }),
    )
}

async fn transaction_defaults_text(client: &tokio_postgres::Client, sql: &str) -> String {
    client.query_one(sql, &[]).await.unwrap().get(0)
}

#[tokio::test]
async fn session_transaction_defaults_preserve_visibility_and_explicit_overrides() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let root = tempfile::tempdir().unwrap();
        let server = Running::with_profile(root.path(), profile);
        let (a, a_task) = transaction_defaults_connect(&server, profile).await;
        let (b, b_task) = transaction_defaults_connect(&server, profile).await;
        a.batch_execute("CREATE TABLE session_rows(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO session_rows VALUES(1,0)").await.unwrap();
        let mode = if profile == "postgres" {
            "read committed"
        } else {
            "serializable"
        };
        a.batch_execute(&format!(
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {mode}"
        ))
        .await
        .unwrap();
        assert_eq!(
            transaction_defaults_text(&a, "SHOW TRANSACTION ISOLATION LEVEL").await,
            mode
        );
        assert_eq!(
            transaction_defaults_text(&b, "SHOW transaction_isolation").await,
            "read committed"
        );
        a.batch_execute("BEGIN").await.unwrap();
        let count = "SELECT CAST(COUNT(*) AS VARCHAR(30)) FROM session_rows";
        assert_eq!(transaction_defaults_text(&a, count).await, "1");
        b.batch_execute("INSERT INTO session_rows VALUES(2,0)")
            .await
            .unwrap();
        assert_eq!(
            transaction_defaults_text(&a, count).await,
            if profile == "postgres" { "2" } else { "1" }
        );
        a.batch_execute("UPDATE session_rows SET v=7 WHERE id=1")
            .await
            .unwrap();
        let value = "SELECT CAST(v AS VARCHAR(30)) FROM session_rows WHERE id=1";
        assert_eq!(transaction_defaults_text(&a, value).await, "7");
        assert_eq!(transaction_defaults_text(&b, value).await, "0");
        a.batch_execute("COMMIT").await.unwrap();
        assert_eq!(transaction_defaults_text(&b, value).await, "7");
        a.batch_execute("BEGIN").await.unwrap();
        assert_eq!(transaction_defaults_text(&a, count).await, "2");
        b.batch_execute("INSERT INTO session_rows VALUES(3,0)")
            .await
            .unwrap();
        assert_eq!(
            transaction_defaults_text(&a, count).await,
            if profile == "postgres" { "3" } else { "2" }
        );
        a.batch_execute("ROLLBACK;BEGIN ISOLATION LEVEL READ COMMITTED")
            .await
            .unwrap();
        assert_eq!(transaction_defaults_text(&a, count).await, "3");
        b.batch_execute("INSERT INTO session_rows VALUES(4,0)")
            .await
            .unwrap();
        assert_eq!(transaction_defaults_text(&a, count).await, "4");
        assert_eq!(
            transaction_defaults_text(&a, "SHOW transaction_isolation").await,
            "read committed"
        );
        a.batch_execute("COMMIT").await.unwrap();
        assert_eq!(
            transaction_defaults_text(&a, "SHOW transaction_isolation").await,
            mode
        );
        if profile == "postgres" {
            let error = a
                .batch_execute(
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                )
                .await
                .unwrap_err();
            assert_eq!(error.code().unwrap().code(), "0A000");
            assert_eq!(
                transaction_defaults_text(&a, "SHOW default_transaction_isolation").await,
                "read committed"
            );
        }
        drop(a);
        drop(b);
        a_task.await.unwrap();
        b_task.await.unwrap();
    }
}

#[tokio::test]
async fn session_transaction_defaults_restore_with_savepoints_reset_and_chain() {
    let root = tempfile::tempdir().unwrap();
    let server = Running::with_profile(root.path(), "oracle-preview-v1");
    let (client, task) = transaction_defaults_connect(&server, "oracle-preview-v1").await;
    client.batch_execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;BEGIN ISOLATION LEVEL READ COMMITTED;SELECT 1 FROM DUAL;COMMIT AND CHAIN").await.unwrap();
    assert_eq!(
        transaction_defaults_text(&client, "SHOW transaction_isolation").await,
        "read committed"
    );
    assert_eq!(
        transaction_defaults_text(&client, "SHOW default_transaction_isolation").await,
        "serializable"
    );
    client
        .batch_execute("SET default_transaction_isolation='read committed';ROLLBACK AND CHAIN")
        .await
        .unwrap();
    assert_eq!(
        transaction_defaults_text(&client, "SHOW transaction_isolation").await,
        "read committed"
    );
    assert_eq!(
        transaction_defaults_text(&client, "SHOW default_transaction_isolation").await,
        "serializable"
    );
    client.batch_execute("COMMIT;BEGIN;SAVEPOINT defaults_mark;SET default_transaction_isolation='read committed';ROLLBACK TO defaults_mark").await.unwrap();
    assert_eq!(
        transaction_defaults_text(&client, "SHOW default_transaction_isolation").await,
        "serializable"
    );
    client
        .batch_execute("SELECT 1 FROM DUAL;RESET ALL")
        .await
        .unwrap();
    assert_eq!(
        transaction_defaults_text(&client, "SHOW transaction_isolation").await,
        "serializable"
    );
    assert_eq!(
        transaction_defaults_text(&client, "SHOW default_transaction_isolation").await,
        "read committed"
    );
    client.batch_execute("COMMIT AND CHAIN").await.unwrap();
    assert_eq!(
        transaction_defaults_text(&client, "SHOW transaction_isolation").await,
        "serializable"
    );
    assert_eq!(
        transaction_defaults_text(
            &client,
            "SELECT current_setting('transaction_isolation') FROM DUAL"
        )
        .await,
        "serializable"
    );
    client.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        transaction_defaults_text(&client, "SHOW transaction_isolation").await,
        "read committed"
    );
    drop(client);
    task.await.unwrap();
}

#[tokio::test]
async fn oracle_session_serializable_default_rejects_stale_updates() {
    let root = tempfile::tempdir().unwrap();
    let server = Running::with_profile(root.path(), "oracle-preview-v1");
    let (a, a_task) = transaction_defaults_connect(&server, "oracle-preview-v1").await;
    let (b, b_task) = transaction_defaults_connect(&server, "oracle-preview-v1").await;
    a.batch_execute("CREATE TABLE stale_session_rows(id INTEGER PRIMARY KEY,v INTEGER);INSERT INTO stale_session_rows VALUES(1,0);SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;BEGIN").await.unwrap();
    assert_eq!(
        transaction_defaults_text(
            &a,
            "SELECT CAST(v AS VARCHAR(30)) FROM stale_session_rows WHERE id=1"
        )
        .await,
        "0"
    );
    b.batch_execute("UPDATE stale_session_rows SET v=1 WHERE id=1")
        .await
        .unwrap();
    let error = a
        .batch_execute("UPDATE stale_session_rows SET v=2 WHERE id=1")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "40001");
    a.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        transaction_defaults_text(
            &b,
            "SELECT CAST(v AS VARCHAR(30)) FROM stale_session_rows WHERE id=1"
        )
        .await,
        "1"
    );
    drop(a);
    drop(b);
    a_task.await.unwrap();
    b_task.await.unwrap();
}

#[tokio::test]
async fn prepared_requests_reject_batches_before_execution_and_preserve_routine_bodies() {
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
        let connection = tokio::spawn(connection);
        client
            .batch_execute("CREATE TABLE prepared_batch_rows(id INTEGER)")
            .await
            .unwrap();
        for sql in [
            "SELECT 1; SELECT 2",
            "INSERT INTO prepared_batch_rows VALUES(1); SELECT 2",
        ] {
            let error = client.prepare(sql).await.unwrap_err();
            assert_eq!(error.code().unwrap().code(), "42601", "{profile}: {sql}");
        }
        let rows = client
            .query(
                "SELECT CAST(COUNT(*) AS VARCHAR(4)) FROM prepared_batch_rows",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows[0].get::<_, String>(0), "0");
        for sql in [
            "SELECT CAST(';' AS VARCHAR(4))",
            "SELECT CAST(';' AS VARCHAR(4)); -- trailing ; comment",
            "/* leading ; comment */ SELECT CAST(';' AS VARCHAR(4)); ;",
        ] {
            let statement = client.prepare(sql).await.unwrap();
            let rows = client.query(&statement, &[]).await.unwrap();
            assert_eq!(rows[0].get::<_, String>(0), ";", "{profile}: {sql}");
        }
        let routine = if profile == "postgres" {
            "CREATE FUNCTION prepared_body_value() RETURNS INTEGER LANGUAGE plpgsql AS $$DECLARE n INTEGER:=1;BEGIN n:=n+2;RETURN n;END$$"
        } else {
            "CREATE FUNCTION prepared_body_value RETURN INTEGER IS n INTEGER:=1;BEGIN n:=n+2;RETURN n;END;"
        };
        client.execute(routine, &[]).await.unwrap();
        let rows = client
            .query("SELECT CAST(prepared_body_value() AS VARCHAR(4))", &[])
            .await
            .unwrap();
        assert_eq!(rows[0].get::<_, String>(0), "3");
        client.batch_execute("BEGIN").await.unwrap();
        let error = client.prepare("SELECT 1; SELECT 2").await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "42601");
        let error = client.query("SELECT 3", &[]).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "25P02", "{profile}");
        client.batch_execute("ROLLBACK").await.unwrap();
        client.query("SELECT 3", &[]).await.unwrap();
        drop(client);
        connection.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn parenthesized_named_queries_describe_columns_before_rows_exist() {
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
        let connection = tokio::spawn(connection);
        client.batch_execute("CREATE TABLE typed_query_rows(id NUMERIC(10,0),amount NUMERIC(10,2),seen TIMESTAMP,label VARCHAR(8))").await.unwrap();
        for query in [
            "((SELECT r.* FROM typed_query_rows r WHERE label=$1 ORDER BY id FETCH FIRST 1 ROW ONLY))",
            "(SELECT r.* FROM typed_query_rows r WHERE label=$1 ORDER BY id FETCH FIRST 1 ROW ONLY) UNION ALL (SELECT r.* FROM typed_query_rows r WHERE label=$1 AND id<0 ORDER BY id FETCH FIRST 1 ROW ONLY)",
        ] {
            client.batch_execute("DELETE FROM typed_query_rows").await.unwrap();
            let statement = client.prepare(query).await.unwrap();
            assert_eq!(statement.columns().len(), 4, "{profile}: {query}");
            let labels: Vec<_> = statement.columns().iter().map(|c| c.name().to_ascii_lowercase()).collect();
            assert_eq!(labels, ["id", "amount", "seen", "label"]);
            let types: Vec<_> = statement.columns().iter().map(|c| c.type_().oid()).collect();
            assert_eq!(types, [1700, 1700, 1114, 1043], "{profile}: {query}");
            assert!(client.query(&statement, &[&"match"]).await.unwrap().is_empty());
            client.batch_execute("INSERT INTO typed_query_rows VALUES(1,12.50,TIMESTAMP '2024-02-29 12:34:56','match')").await.unwrap();
            for _ in 0..10 {
                let rows = client.query(&statement, &[&"match"]).await.unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 4);
                assert_eq!(rows[0].get::<_, String>(3), "match");
                assert!(client.query(&statement, &[&"missing"]).await.unwrap().is_empty());
            }
        }
        drop(client);
        connection.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn fixed_number_pictures_keep_profile_rules_for_prepared_parameters() {
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
        let connection = tokio::spawn(connection);
        let statement = client
            .prepare("SELECT TO_CHAR(CAST(CAST($1 AS TEXT) AS NUMERIC),CAST($2 AS TEXT))")
            .await
            .unwrap();
        assert_eq!(
            statement.columns()[0].type_(),
            &tokio_postgres::types::Type::TEXT
        );
        for _ in 0..3 {
            for (value, model, oracle, postgres) in [
                ("12.5", "9G990D00", "    12.50", "    12.50"),
                ("1234.56", "FM9G990D00", "1,234.56", "1,234.56"),
                ("-0.004", "FM9G990D00", "-0.00", "0.00"),
                ("9999.995", "FM9G990D00", "#########", "#,###.##"),
                ("12.5", "000G000D00", " 000,012.50", " 000,012.50"),
            ] {
                let row = client
                    .query_one(&statement, &[&value, &model])
                    .await
                    .unwrap();
                assert_eq!(
                    row.get::<_, String>(0),
                    if profile == "postgres" {
                        postgres
                    } else {
                        oracle
                    },
                    "{profile}: {value} {model}"
                );
            }
            let empty: Option<&str> = None;
            let row = client
                .query_one(&statement, &[&empty, &"9G990D00"])
                .await
                .unwrap();
            assert_eq!(row.get::<_, Option<String>>(0), None);
        }
        drop(client);
        connection.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn set_operand_builtins_keep_prepared_values_and_descriptions_after_reopen() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let directory = tempfile::tempdir().unwrap();
        for reopen in [false, true] {
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
            let connection = tokio::spawn(connection);
            if !reopen {
                client.batch_execute("CREATE TABLE operand_driver_groups(id NUMERIC PRIMARY KEY); CREATE TABLE operand_driver_numbers(row_id INTEGER PRIMARY KEY,group_id NUMERIC,amount NUMERIC); INSERT INTO operand_driver_groups VALUES(1),(2); INSERT INTO operand_driver_numbers VALUES(1,1,NULL),(2,1,7)").await.unwrap();
                let function = if profile == "postgres" {
                    "COALESCE"
                } else {
                    "NVL"
                };
                client.batch_execute(&format!("CREATE VIEW operand_driver_view AS SELECT id,CAST(0 AS NUMERIC) AS qty FROM operand_driver_groups WHERE id<0 UNION ALL SELECT g.id,SUM({function}(n.amount,0)) FROM operand_driver_groups g LEFT JOIN operand_driver_numbers n ON g.id=n.group_id GROUP BY g.id")).await.unwrap();
            }
            let rows_statement = client.prepare("SELECT CAST(qty AS TEXT) AS qty FROM operand_driver_view WHERE id=CAST(CAST($1 AS TEXT) AS NUMERIC)").await.unwrap();
            assert_eq!(
                rows_statement.columns()[0].type_(),
                &tokio_postgres::types::Type::TEXT
            );
            let count_statement = client.prepare("SELECT COUNT(*) FROM (SELECT * FROM operand_driver_view WHERE id=CAST(CAST($1 AS TEXT) AS NUMERIC)) counted").await.unwrap();
            for _ in 0..5 {
                for (id, expected) in [("1", Some("7")), ("2", Some("0")), ("9", None)] {
                    let rows = client.query(&rows_statement, &[&id]).await.unwrap();
                    assert_eq!(
                        rows.len(),
                        usize::from(expected.is_some()),
                        "{profile} reopen={reopen}"
                    );
                    if let Some(expected) = expected {
                        assert_eq!(rows[0].get::<_, String>(0), expected);
                    }
                    let count = client.query_one(&count_statement, &[&id]).await.unwrap();
                    assert_eq!(count.get::<_, i64>(0), i64::from(expected.is_some()));
                }
            }
            drop(client);
            connection.await.unwrap().unwrap();
        }
    }
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
async fn preparing_typed_wildcards_does_not_advance_sequences_in_either_profile() {
    for oracle in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let server = Running::new(directory.path(), oracle);
        let (client, connection) = server.connect(oracle).await.unwrap();
        client.batch_execute("CREATE TABLE describe_rows(id INTEGER,marker VARCHAR(8)); INSERT INTO describe_rows VALUES(1,'visible'); CREATE SEQUENCE describe_counter;").await.unwrap();
        client.batch_execute(r#"CREATE TABLE "QuotedRows"("Key" INTEGER,"Value" VARCHAR(8)); CREATE VIEW quoted_labels("ResultKey","ResultValue") AS SELECT * FROM "QuotedRows""#).await.unwrap();
        for (sql, names) in [
            (r#"SELECT * FROM "QuotedRows""#, vec!["Key", "Value"]),
            (
                r#"SELECT d.* FROM (SELECT * FROM "QuotedRows") d"#,
                vec!["Key", "Value"],
            ),
            (
                r#"WITH d AS (SELECT * FROM "QuotedRows") SELECT * FROM d"#,
                vec!["Key", "Value"],
            ),
            (
                "SELECT * FROM quoted_labels",
                vec!["ResultKey", "ResultValue"],
            ),
        ] {
            let statement = client.prepare(sql).await.unwrap();
            assert_eq!(
                statement
                    .columns()
                    .iter()
                    .map(|column| column.name())
                    .collect::<Vec<_>>(),
                names,
                "{sql}"
            );
        }
        let expression = if oracle {
            "describe_counter.NEXTVAL"
        } else {
            "nextval('describe_counter')"
        };
        for wildcard in ["*", "r.*"] {
            let statement = client
                .prepare(&format!(
                    "SELECT {wildcard},{expression} AS counter FROM describe_rows r"
                ))
                .await
                .unwrap();
            assert_eq!(statement.columns().len(), 3);
            assert_eq!(statement.columns()[0].name().to_ascii_lowercase(), "id");
            assert_eq!(statement.columns()[1].name().to_ascii_lowercase(), "marker");
            assert_eq!(
                statement.columns()[2].name().to_ascii_lowercase(),
                "counter"
            );
        }
        let sql = if oracle {
            "SELECT describe_counter.NEXTVAL FROM DUAL"
        } else {
            "SELECT nextval('describe_counter')"
        };
        let result = client.simple_query(sql).await.unwrap();
        let row = result
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .unwrap();
        assert_eq!(row.get(0), Some("1"));
        drop(client);
        connection.await.unwrap();
        drop(server);
    }
}

#[tokio::test]
async fn wildcard_join_describe_and_binary_values_follow_logical_columns_in_both_profiles() {
    use std::time::{Duration, SystemTime};
    use tokio_postgres::types::Type;
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
        let connection = tokio::spawn(async move { connection.await.unwrap() });
        let varchar = if profile == "postgres" {
            "VARCHAR(8)"
        } else {
            "VARCHAR2(8)"
        };
        client.batch_execute(&format!("CREATE TABLE wire_left(key_id {varchar},time_value TIMESTAMP,marker {varchar});CREATE TABLE wire_right(key_id {varchar},marker {varchar})")).await.unwrap();
        for key in 1..=32 {
            client.batch_execute(&format!("INSERT INTO wire_left VALUES('{key:02}',TIMESTAMP '2024-02-29 12:34:56.123456','left_{key:02}')")).await.unwrap();
        }
        client
            .batch_execute("INSERT INTO wire_right VALUES('07','right')")
            .await
            .unwrap();
        let sql = "SELECT * FROM wire_left l JOIN wire_right r ON l.key_id=r.key_id";
        let statement = client.prepare(sql).await.unwrap();
        let expected_names = vec!["key_id", "time_value", "marker", "key_id", "marker"];
        let expected_types = vec![
            Type::VARCHAR,
            Type::TIMESTAMP,
            Type::VARCHAR,
            Type::VARCHAR,
            Type::VARCHAR,
        ];
        assert_eq!(
            statement
                .columns()
                .iter()
                .map(|c| c.name().to_ascii_lowercase())
                .collect::<Vec<_>>(),
            expected_names,
            "{profile}"
        );
        assert_eq!(
            statement
                .columns()
                .iter()
                .map(|c| c.type_().clone())
                .collect::<Vec<_>>(),
            expected_types,
            "{profile}"
        );
        let row = client.query_one(&statement, &[]).await.unwrap();
        assert_eq!(row.get::<_, String>(0), "07");
        assert_eq!(
            row.get::<_, SystemTime>(1),
            SystemTime::UNIX_EPOCH + Duration::from_micros(1_709_210_096_123_456)
        );
        assert_eq!(row.get::<_, String>(2), "left_07");
        assert_eq!(row.get::<_, String>(3), "07");
        assert_eq!(row.get::<_, String>(4), "right");
        let empty = client.prepare(&format!("{sql} WHERE 1=0")).await.unwrap();
        assert_eq!(
            empty
                .columns()
                .iter()
                .map(|c| c.type_().clone())
                .collect::<Vec<_>>(),
            expected_types
        );
        assert!(client.query(&empty, &[]).await.unwrap().is_empty());
        for ordering in ["3 DESC,1 ASC", "5 ASC,1 DESC"] {
            let statement=client.prepare(&format!("SELECT d.* FROM (SELECT * FROM wire_left l CROSS JOIN wire_right r) d ORDER BY {ordering} FETCH FIRST 1 ROWS ONLY")).await.unwrap();
            assert_eq!(
                statement
                    .columns()
                    .iter()
                    .map(|c| c.type_().clone())
                    .collect::<Vec<_>>(),
                expected_types
            );
            let row = client.query_one(&statement, &[]).await.unwrap();
            assert_eq!(row.get::<_, String>(0), "32", "{profile}: {ordering}");
            assert_eq!(row.get::<_, String>(2), "left_32", "{profile}: {ordering}");
            assert_eq!(row.get::<_, String>(3), "07");
            assert_eq!(row.get::<_, String>(4), "right");
        }
        let integer = if profile == "postgres" {
            "INTEGER"
        } else {
            "NUMBER(9)"
        };
        client.batch_execute(&format!("CREATE TABLE ordinal_left(left_key {integer});CREATE TABLE ordinal_right(right_key {integer})")).await.unwrap();
        for key in 1..=32 {
            client
                .batch_execute(&format!("INSERT INTO ordinal_left VALUES({key})"))
                .await
                .unwrap();
        }
        client
            .batch_execute("INSERT INTO ordinal_right VALUES(99)")
            .await
            .unwrap();
        let result = client.simple_query("SELECT * FROM ordinal_left l CROSS JOIN ordinal_right r ORDER BY 1 DESC FETCH FIRST 1 ROWS ONLY").await.unwrap();
        let row = result
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .unwrap();
        assert_eq!(row.get(0), Some("32"), "{profile}");
        assert_eq!(row.get(1), Some("99"), "{profile}");
        drop(client);
        connection.await.unwrap();
    }
}

#[tokio::test]
async fn preview_current_time_has_typed_nonnull_prepared_binary_values() {
    use std::time::{Duration, SystemTime};
    use tokio_postgres::types::Type;
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(server.port)
        .user("bicdb")
        .dbname("bicdb")
        .options("-c bicdb.sql_profile=oracle-preview-v1");
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(async move { connection.await.unwrap() });
    let statement = client
        .prepare("SELECT CURRENT_TIMESTAMP,LOCALTIMESTAMP,CURRENT_DATE FROM DUAL")
        .await
        .unwrap();
    let types: Vec<_> = statement
        .columns()
        .iter()
        .map(|c| c.type_().clone())
        .collect();
    assert_eq!(
        types,
        vec![Type::TIMESTAMPTZ, Type::TIMESTAMP, Type::TIMESTAMP]
    );
    client.batch_execute("BEGIN").await.unwrap();
    let row = client.query_one(&statement, &[]).await.unwrap();
    let instant: SystemTime = row.get(0);
    assert!(SystemTime::now().duration_since(instant).unwrap() < Duration::from_secs(5));
    assert_eq!(instant, row.get::<_, SystemTime>(1));
    let date: SystemTime = row.get(2);
    assert!(instant.duration_since(date).unwrap() < Duration::from_secs(1));
    tokio::time::sleep(Duration::from_millis(30)).await;
    let later: SystemTime = client.query_one(&statement, &[]).await.unwrap().get(0);
    assert!(later > instant);
    client.batch_execute("ROLLBACK").await.unwrap();
    drop(client);
    connection.await.unwrap();

    let native_dir = tempfile::tempdir().unwrap();
    let native = Running::new(native_dir.path(), false);
    let (client, connection) = native.connect(false).await.unwrap();
    let statement = client
        .prepare("SELECT CURRENT_TIMESTAMP,CURRENT_DATE,clock_timestamp()")
        .await
        .unwrap();
    assert_eq!(
        statement
            .columns()
            .iter()
            .map(|c| c.type_().clone())
            .collect::<Vec<_>>(),
        vec![Type::TIMESTAMPTZ, Type::DATE, Type::TIMESTAMPTZ]
    );
    client.batch_execute("BEGIN").await.unwrap();
    let first: SystemTime = client.query_one(&statement, &[]).await.unwrap().get(0);
    tokio::time::sleep(Duration::from_millis(30)).await;
    let second: SystemTime = client.query_one(&statement, &[]).await.unwrap().get(0);
    assert_eq!(first, second);
    client.batch_execute("ROLLBACK").await.unwrap();
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn preview_bounded_locks_preserve_prepared_describe_cancel_and_connection_reuse() {
    use std::time::Duration;
    use tokio_postgres::types::Type;
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(server.port)
        .user("bicdb")
        .dbname("bicdb")
        .options("-c bicdb.sql_profile=oracle-preview-v1");
    let (owner, owner_connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    let owner_connection = tokio::spawn(async move { owner_connection.await.unwrap() });
    let (waiter, waiter_connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    let waiter_connection = tokio::spawn(async move { waiter_connection.await.unwrap() });
    owner.batch_execute("CREATE TABLE wire_deadlines(id NUMBER PRIMARY KEY); INSERT INTO wire_deadlines VALUES(1)").await.unwrap();
    owner.batch_execute("BEGIN").await.unwrap();
    owner
        .query("SELECT id FROM wire_deadlines WHERE id=1 FOR UPDATE", &[])
        .await
        .unwrap();
    let sql = "SELECT CAST(id AS VARCHAR2(30)) FROM wire_deadlines WHERE id=$1 FOR UPDATE WAIT 1";
    let prepared = tokio::time::timeout(
        Duration::from_secs(1),
        waiter.prepare_typed(sql, &[Type::INT4]),
    )
    .await
    .expect("Describe must not wait for the row lock")
    .unwrap();
    assert_eq!(*prepared.columns()[0].type_(), Type::VARCHAR);
    waiter.batch_execute("BEGIN").await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(3), waiter.query(&prepared, &[&1_i32]))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "55P03");
    waiter.batch_execute("ROLLBACK").await.unwrap();

    let prepared_cancel = waiter
        .prepare_typed(&sql.replace("WAIT 1", "WAIT 5"), &[Type::INT4])
        .await
        .unwrap();
    waiter.batch_execute("BEGIN").await.unwrap();
    let cancel = waiter.cancel_token();
    let (result, ()) = tokio::join!(waiter.query(&prepared_cancel, &[&1_i32]), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel_query(tokio_postgres::NoTls).await.unwrap();
    });
    assert_eq!(result.unwrap_err().code().unwrap().code(), "57014");
    waiter.batch_execute("ROLLBACK").await.unwrap();
    owner.batch_execute("COMMIT").await.unwrap();
    let row = waiter.query_one(&prepared, &[&1_i32]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "1");
    drop(owner);
    drop(waiter);
    owner_connection.await.unwrap();
    waiter_connection.await.unwrap();

    let native_dir = tempfile::tempdir().unwrap();
    let native = Running::new(native_dir.path(), false);
    let (client, connection) = native.connect(false).await.unwrap();
    assert_eq!(
        client
            .prepare("SELECT 1 FOR UPDATE WAIT 1")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "42601"
    );
    assert_eq!(
        client
            .query_one("SELECT 1", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn qualified_scalar_aggregates_keep_prepared_types_with_empty_results() {
    use tokio_postgres::types::Type;
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
        client.batch_execute(r#"CREATE SCHEMA "typed.scope"; CREATE TABLE "public"."outer_keys"("k" INTEGER); CREATE TABLE "typed.scope"."amount_rows"("k" INTEGER,"amount" NUMERIC(20,2)); INSERT INTO "public"."outer_keys" VALUES(1); INSERT INTO "typed.scope"."amount_rows" VALUES(1,123.50)"#).await.unwrap();
        let sql = r#"SELECT (SELECT COALESCE(SUM(l."amount"),0) FROM "typed.scope"."amount_rows" l WHERE h."k"=l."k") FROM "public"."outer_keys" h WHERE h."k"=$1"#;
        let statement = client.prepare_typed(sql, &[Type::INT4]).await.unwrap();
        assert_eq!(*statement.columns()[0].type_(), Type::NUMERIC, "{profile}");
        let rows = client.query(&statement, &[&1_i32]).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].columns()[0].type_(), Type::NUMERIC);
        assert!(client
            .query(&statement, &[&9_i32])
            .await
            .unwrap()
            .is_empty());
        let messages = client.simple_query(&sql.replace("$1", "1")).await.unwrap();
        let values: Vec<_> = messages
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(0).map(str::to_string))
                }
                _ => None,
            })
            .collect();
        assert_eq!(values, vec![Some("123.50".into())]);
        drop(client);
        connection.await.unwrap();
    }
}

#[tokio::test]
async fn query_logs_identify_connections_and_transaction_state() {
    let directory = tempfile::tempdir().unwrap();
    let log = directory.path().join("queries.jsonl");
    let config = PgWireConfig {
        fsync: false,
        slow_query_log: Some(log.clone()),
        slow_query_threshold: std::time::Duration::ZERO,
        ..Default::default()
    };
    let engine = PgWireServer::open(directory.path().join("db"), config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let copy = engine.clone();
    let server = Running {
        server: engine,
        port,
        thread: Some(thread::spawn(move || {
            bicdb_pgwire::serve_existing_listener(copy, listener)
        })),
    };
    let (first, first_task) = server.connect(false).await.unwrap();
    let (second, second_task) = server.connect(false).await.unwrap();
    first.batch_execute("SELECT 17").await.unwrap();
    second
        .batch_execute("BEGIN; SELECT 18; ROLLBACK")
        .await
        .unwrap();
    drop(first);
    drop(second);
    first_task.await.unwrap();
    second_task.await.unwrap();
    let entries: Vec<serde_json::Value> = std::fs::read_to_string(log)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(entries
        .iter()
        .all(|entry| entry["fields"]["connection_id"].as_u64().is_some()));
    let ids: std::collections::BTreeSet<_> = entries
        .iter()
        .map(|entry| entry["fields"]["connection_id"].as_u64().unwrap())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(entries
        .iter()
        .any(|entry| entry["fields"]["query"] == "BEGIN"
            && entry["fields"]["transaction_active"] == true));
    assert!(entries
        .iter()
        .any(|entry| entry["fields"]["query"] == "ROLLBACK"
            && entry["fields"]["transaction_active"] == false));
}

#[tokio::test]
async fn anchored_types_keep_owner_precision_and_driver_types_after_reopen() {
    use tokio_postgres::types::Type;
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
                client.batch_execute(r#"CREATE SCHEMA "left_scope"; CREATE SCHEMA "right_scope"; CREATE TABLE "public"."anchor_rows"("value" VARCHAR(1)); CREATE TABLE "left_scope"."anchor_rows"("value" NUMERIC(5,2)); CREATE TABLE "right_scope"."anchor_rows"("value" NUMERIC(6,3))"#).await.unwrap();
                client
                    .batch_execute(r#"SET search_path='"left_scope"'"#)
                    .await
                    .unwrap();
                let selected = if profile == "postgres" {
                    "CREATE FUNCTION selected_anchor(p_value NUMERIC) RETURNS NUMERIC LANGUAGE plpgsql AS $$DECLARE result NUMERIC(5,2); BEGIN result:=p_value; RETURN result; END$$"
                } else {
                    r#"CREATE FUNCTION selected_anchor(p_value NUMBER) RETURN NUMBER IS result "anchor_rows"."value"%TYPE; BEGIN result:=p_value; RETURN result; END;"#
                };
                client.batch_execute(selected).await.unwrap();
                client
                    .batch_execute(r#"SET search_path='"right_scope"'"#)
                    .await
                    .unwrap();
                let owned = if profile == "postgres" {
                    r#"CREATE FUNCTION "left_scope".owner_anchor(p_value NUMERIC) RETURNS NUMERIC LANGUAGE plpgsql AS $$DECLARE result NUMERIC(5,2); BEGIN result:=p_value; RETURN result; END$$"#
                } else {
                    r#"CREATE FUNCTION "left_scope".owner_anchor(p_value NUMBER) RETURN NUMBER IS result "anchor_rows"."value"%TYPE; BEGIN result:=p_value; RETURN result; END;"#
                };
                client.batch_execute(owned).await.unwrap();
                let returned = if profile == "postgres" {
                    r#"CREATE FUNCTION "left_scope".return_anchor(p_value NUMERIC) RETURNS NUMERIC(5,2) LANGUAGE plpgsql AS $$BEGIN RETURN p_value; END$$"#
                } else {
                    r#"CREATE FUNCTION "left_scope".return_anchor(p_value NUMBER) RETURN "anchor_rows"."value"%TYPE IS BEGIN RETURN p_value; END;"#
                };
                client.batch_execute(returned).await.unwrap();
            }
            client
                .batch_execute(r#"SET search_path='"right_scope"'"#)
                .await
                .unwrap();
            let suffix = if profile == "postgres" {
                ""
            } else {
                " FROM DUAL"
            };
            for name in ["selected_anchor", "owner_anchor", "return_anchor"] {
                let native_unconstrained_return = profile == "postgres" && name == "return_anchor";
                let sql = format!(
                    r#"SELECT CAST("left_scope".{name}(CAST(CAST($1 AS VARCHAR) AS NUMERIC)) AS VARCHAR(40)){suffix}"#
                );
                let statement = client.prepare(&sql).await.unwrap();
                assert_eq!(statement.columns()[0].type_(), &Type::VARCHAR);
                assert_eq!(
                    client
                        .query_one(&statement, &[&"12.345"])
                        .await
                        .unwrap()
                        .get::<_, String>(0),
                    if native_unconstrained_return {
                        "12.345"
                    } else {
                        "12.35"
                    }
                );
                let null: Option<String> = None;
                assert!(client
                    .query_one(&statement, &[&null])
                    .await
                    .unwrap()
                    .get::<_, Option<String>>(0)
                    .is_none());
                if native_unconstrained_return {
                    assert_eq!(
                        client
                            .query_one(&statement, &[&"1000.123"])
                            .await
                            .unwrap()
                            .get::<_, String>(0),
                        "1000.123"
                    );
                } else {
                    let error = client.query_one(&statement, &[&"1000"]).await.unwrap_err();
                    assert_eq!(
                        error.code(),
                        Some(&tokio_postgres::error::SqlState::NUMERIC_VALUE_OUT_OF_RANGE)
                    );
                }
                assert_eq!(
                    client
                        .query_one(&statement, &[&"-12.345"])
                        .await
                        .unwrap()
                        .get::<_, String>(0),
                    if native_unconstrained_return {
                        "-12.345"
                    } else {
                        "-12.35"
                    }
                );
            }
            drop(client);
            connection.await.unwrap();
        }
    }
}

#[tokio::test]
async fn week_and_null_formats_keep_profile_semantics_after_reopen() {
    use tokio_postgres::types::Type;
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
                let ddl = if profile == "postgres" {
                    "CREATE FUNCTION week_clip(p_date TIMESTAMP,p_fmt TEXT) RETURNS TIMESTAMP LANGUAGE plpgsql AS $$BEGIN RETURN date_trunc(p_fmt,p_date); END$$"
                } else {
                    "CREATE FUNCTION week_clip(p_date DATE,p_fmt VARCHAR2) RETURN DATE IS BEGIN RETURN TRUNC(p_date,p_fmt); END;"
                };
                client.batch_execute(ddl).await.unwrap();
                if profile != "postgres" {
                    client.batch_execute("CREATE FUNCTION sql_clip(p_date DATE,p_fmt VARCHAR2) RETURN DATE IS result DATE; BEGIN SELECT TRUNC(p_date,p_fmt) INTO result FROM DUAL; RETURN result; END;").await.unwrap();
                }
            }
            let suffix = if profile == "postgres" {
                ""
            } else {
                " FROM DUAL"
            };
            let query =
                format!("SELECT week_clip(CAST(CAST($1 AS VARCHAR) AS TIMESTAMP),$2){suffix}");
            let statement = client.prepare(&query).await.unwrap();
            assert_eq!(statement.columns()[0].type_(), &Type::TIMESTAMP);
            let date = Some("2024-02-29 12:34:56.9".to_string());
            let format = Some(if profile == "postgres" {
                "week".to_string()
            } else {
                "D".to_string()
            });
            let row = client
                .query_one(&statement, &[&date, &format])
                .await
                .unwrap();
            let seconds = if profile == "postgres" {
                1_708_905_600
            } else {
                1_708_819_200
            };
            assert_eq!(
                row.get::<_, std::time::SystemTime>(0),
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds)
            );
            let null: Option<String> = None;
            if profile == "postgres" {
                assert!(client
                    .query_one(&statement, &[&date, &null])
                    .await
                    .unwrap()
                    .get::<_, Option<std::time::SystemTime>>(0)
                    .is_none());
            } else {
                let error = client
                    .query_one(&statement, &[&date, &null])
                    .await
                    .unwrap_err();
                assert_eq!(
                    error.code(),
                    Some(&tokio_postgres::error::SqlState::INVALID_PARAMETER_VALUE)
                );
                let sql = client
                    .prepare(&format!(
                        "SELECT sql_clip(CAST(CAST($1 AS VARCHAR) AS TIMESTAMP),$2){suffix}"
                    ))
                    .await
                    .unwrap();
                assert!(client
                    .query_one(&sql, &[&date, &null])
                    .await
                    .unwrap()
                    .get::<_, Option<std::time::SystemTime>>(0)
                    .is_none());
                let scalar = client
                    .prepare(&format!(
                        "SELECT TRUNC(CAST(CAST($1 AS VARCHAR) AS TIMESTAMP),$2){suffix}"
                    ))
                    .await
                    .unwrap();
                assert!(client
                    .query_one(&scalar, &[&date, &null])
                    .await
                    .unwrap()
                    .get::<_, Option<std::time::SystemTime>>(0)
                    .is_none());
            }
            assert!(client
                .query_one(&statement, &[&null, &null])
                .await
                .unwrap()
                .get::<_, Option<std::time::SystemTime>>(0)
                .is_none());
            assert!(client
                .query_one(&statement, &[&date, &format])
                .await
                .is_ok());
            drop(client);
            connection.await.unwrap();
        }
    }
}

#[tokio::test]
async fn calendar_routines_keep_profile_semantics_in_prepared_calls_after_reopen() {
    use tokio_postgres::types::Type;
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
                let source = if profile == "postgres" {
                    "CREATE FUNCTION calendar_shift(p_date TIMESTAMP,p_months NUMERIC) RETURNS TIMESTAMP LANGUAGE plpgsql AS $$BEGIN RETURN p_date+INTERVAL '1 month'; END$$"
                } else {
                    "CREATE FUNCTION calendar_shift(p_date DATE,p_months NUMBER) RETURN DATE IS BEGIN RETURN ADD_MONTHS(p_date,p_months); END;"
                };
                client.batch_execute(source).await.unwrap();
            }
            let mut query="SELECT calendar_shift(CAST(CAST($1 AS VARCHAR) AS TIMESTAMP),CAST(CAST($2 AS VARCHAR) AS NUMERIC))".to_string();
            if profile != "postgres" {
                query.push_str(" FROM DUAL");
            }
            let statement = client.prepare(&query).await.unwrap();
            assert_eq!(statement.columns()[0].type_(), &Type::TIMESTAMP);
            let row = client
                .query_one(
                    &statement,
                    &[&"2024-02-29 12:34:56.9".to_string(), &"1".to_string()],
                )
                .await
                .unwrap();
            // Native interval addition preserves .9 seconds on March 29;
            // Oracle's DATE month-end rule returns March 31, whole seconds.
            let expected = std::time::UNIX_EPOCH
                + if profile == "postgres" {
                    std::time::Duration::from_secs(1_711_715_696)
                        + std::time::Duration::from_millis(900)
                } else {
                    std::time::Duration::from_secs(1_711_888_496)
                };
            assert_eq!(row.get::<_, std::time::SystemTime>(0), expected);
            drop(client);
            connection.await.unwrap();
        }
    }
}

#[tokio::test]
async fn interval_text_settings_apply_within_batches_in_both_profiles() {
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
        assert_eq!(connection.parameter("IntervalStyle"), Some("postgres"));
        let connection = tokio::spawn(async move {
            connection.await.unwrap();
        });
        for (sql, expected) in [
            (
                "SET intervalstyle = 'iso_8601'; SELECT INTERVAL '1 second'",
                "PT1S",
            ),
            (
                "SET intervalstyle = 'sql_standard'; SELECT INTERVAL '1 second'",
                "0:00:01",
            ),
            (
                "RESET intervalstyle; SELECT INTERVAL '1 second'",
                "00:00:01",
            ),
        ] {
            let messages = client.simple_query(sql).await.unwrap();
            let values: Vec<_> = messages
                .iter()
                .filter_map(|message| {
                    if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
                        row.get(0)
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(values, vec![expected], "{profile}: {sql}");
        }
        drop(client);
        connection.await.unwrap();
    }
}

#[tokio::test]
async fn date_cast_semantics_and_metadata_are_profile_specific_after_reopen() {
    use tokio_postgres::types::Type;
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
            assert_eq!(connection.parameter("IntervalStyle"), Some("postgres"));
            let connection = tokio::spawn(async move {
                connection.await.unwrap();
            });
            if attempt == 0 {
                let source = if profile == "postgres" {
                    "CREATE FUNCTION cast_date_value(p_stamp TIMESTAMP) RETURNS DATE LANGUAGE plpgsql AS $$BEGIN RETURN CAST(p_stamp AS DATE); END$$"
                } else {
                    "CREATE FUNCTION cast_date_value(p_stamp TIMESTAMP) RETURN DATE IS BEGIN RETURN CAST(p_stamp AS DATE); END;"
                };
                client.batch_execute(source).await.unwrap();
            }
            let query = if profile == "postgres" {
                "SELECT cast_date_value($1)"
            } else {
                "SELECT cast_date_value($1) FROM DUAL"
            };
            let description = client
                .prepare_typed(query, &[Type::TIMESTAMP])
                .await
                .unwrap();
            assert_eq!(
                *description.columns()[0].type_(),
                if profile == "postgres" {
                    Type::DATE
                } else {
                    Type::TIMESTAMP
                }
            );
            let query = if profile == "postgres" {
                "SELECT CAST(cast_date_value($1) AS VARCHAR(40))"
            } else {
                "SELECT CAST(cast_date_value($1) AS VARCHAR2(40)) FROM DUAL"
            };
            let query = client
                .prepare_typed(query, &[Type::TIMESTAMP])
                .await
                .unwrap();
            for (seconds, expected) in [
                (1709251199, "2024-02-29 23:59:59"),
                (1709251200, "2024-03-01 00:00:00"),
                (1709251199, "2024-02-29 23:59:59"),
            ] {
                let value = std::time::UNIX_EPOCH + std::time::Duration::new(seconds, 900_000_000);
                let actual = client
                    .query_one(&query, &[&value])
                    .await
                    .unwrap()
                    .get::<_, String>(0);
                assert_eq!(
                    actual,
                    if profile == "postgres" {
                        &expected[..10]
                    } else {
                        expected
                    }
                );
            }
            drop(client);
            connection.await.unwrap();
        }
    }
}

#[tokio::test]
async fn oracle_routine_namespace_survives_reopen_and_public_shadow_tables() {
    use tokio_postgres::types::Type;
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
                r#"CREATE SCHEMA "App.Space""#,
                r#"CREATE TABLE "App.Space".scope_values(bucket NUMBER,v NUMBER)"#,
                r#"INSERT INTO "App.Space".scope_values VALUES(7,11),(7,22),(8,44)"#,
                "CREATE TABLE scope_values(bucket NUMBER,v NUMBER)",
                "INSERT INTO scope_values VALUES(7,999)",
                r#"CREATE FUNCTION "App.Space".namespace_total(p_bucket NUMBER) RETURN NUMBER IS total NUMBER:=0; CURSOR c IS SELECT v FROM scope_values WHERE bucket=p_bucket; BEGIN FOR r IN c LOOP total:=total+r.v; END LOOP; RETURN total; END;"#,
            ] {
                client.batch_execute(source).await.unwrap();
            }
        }
        let query = client
            .prepare_typed(
                r#"SELECT CAST("App.Space".namespace_total($1) AS VARCHAR2(20)) FROM DUAL"#,
                &[Type::INT4],
            )
            .await
            .unwrap();
        for (bucket, expected) in [(7i32, "33"), (8, "44"), (9, "0"), (7, "33")] {
            assert_eq!(
                client
                    .query_one(&query, &[&bucket])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                expected
            );
        }
        drop(client);
        connection.await.unwrap();
    }
}

#[tokio::test]
async fn bound_cursor_loops_rebind_driver_parameters_after_restart() {
    use tokio_postgres::types::Type;
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
                client
                    .batch_execute("CREATE TABLE cursor_input(k INT,bucket INT,v INT)")
                    .await
                    .unwrap();
                client
                    .batch_execute("INSERT INTO cursor_input VALUES(1,7,11),(2,7,22),(3,8,44)")
                    .await
                    .unwrap();
                let source = if profile == "postgres" {
                    "CREATE FUNCTION cursor_total(p_bucket INT) RETURNS INT LANGUAGE plpgsql AS $$DECLARE total INT:=0; c CURSOR FOR SELECT v FROM cursor_input WHERE bucket=p_bucket ORDER BY k; BEGIN FOR r IN c LOOP total:=total+r.v; END LOOP; RETURN total; END$$"
                } else {
                    "CREATE FUNCTION cursor_total(p_bucket IN NUMBER) RETURN NUMBER IS total NUMBER:=0; CURSOR c IS SELECT v FROM cursor_input WHERE bucket=p_bucket ORDER BY k; BEGIN FOR r IN c LOOP total:=total+r.v; END LOOP; RETURN total; END;"
                };
                client.batch_execute(source).await.unwrap();
            }
            let query = if profile == "postgres" {
                "SELECT CAST(cursor_total($1) AS VARCHAR(20))"
            } else {
                "SELECT CAST(cursor_total($1) AS VARCHAR2(20)) FROM DUAL"
            };
            let statement = client.prepare_typed(query, &[Type::INT4]).await.unwrap();
            for (bucket, expected) in [(7i32, "33"), (8, "44"), (9, "0"), (7, "33")] {
                assert_eq!(
                    client
                        .query_one(&statement, &[&bucket])
                        .await
                        .unwrap()
                        .get::<_, String>(0),
                    expected,
                    "{profile}: {attempt}"
                );
            }
            drop(client);
            connection.await.unwrap();
        }
    }
}

#[tokio::test]
async fn routine_drop_preserves_owner_checks_and_rollback_over_the_driver() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let directory = tempfile::tempdir().unwrap();
        // Administrative fixture provisioning uses the native API. Oracle
        // preview role DDL/procedure creation are not qualified by this test.
        {
            let mut db = bicdb_core::BicDb::open(directory.path()).unwrap();
            bicdb_sql::oracle::ensure_database_profile(&mut db, profile).unwrap();
            let mut session = bicdb_sql::SqlSession::new(&mut db);
            for source in [
                "CREATE ROLE routine_owner LOGIN", "CREATE ROLE routine_other LOGIN",
                "CREATE SCHEMA routine_space AUTHORIZATION routine_owner", "SET ROLE routine_owner",
                "CREATE FUNCTION routine_space.drop_fn() RETURNS INT LANGUAGE plpgsql AS $$BEGIN RETURN 7; END$$",
                "CREATE PROCEDURE routine_space.drop_proc(p_in INT) LANGUAGE plpgsql AS $$BEGIN RETURN; END$$",
                "GRANT EXECUTE ON FUNCTION drop_fn() TO routine_other"
            ] { session.execute(source).unwrap(); }
        }
        let server = Running::with_profile(directory.path(), profile);
        let connect = |user: &str| {
            let mut config = tokio_postgres::Config::new();
            config
                .host("127.0.0.1")
                .port(server.port)
                .user(user)
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
        let (owner, owner_connection) = connect("routine_owner").await;
        let (other, other_connection) = connect("routine_other").await;
        let query = if profile == "postgres" {
            "SELECT CAST(drop_fn() AS VARCHAR(20))"
        } else {
            "SELECT CAST(drop_fn() AS VARCHAR2(20)) FROM DUAL"
        };
        assert_eq!(
            other
                .query_one(query, &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "7"
        );
        let drops = [
            r#"DROP FUNCTION "routine_space".drop_fn"#,
            r#"DROP PROCEDURE "routine_space".drop_proc"#,
        ];
        for source in drops {
            assert_eq!(
                other
                    .batch_execute(source)
                    .await
                    .unwrap_err()
                    .as_db_error()
                    .unwrap()
                    .code(),
                &tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE
            );
        }
        for source in [
            r#"DROP FUNCTION "wrong_space".drop_fn"#,
            r#"DROP PROCEDURE "wrong_space".drop_proc"#,
        ] {
            assert!(owner.batch_execute(source).await.is_err());
        }
        owner.batch_execute(r#"DROP FUNCTION IF EXISTS "wrong_space".drop_fn CASCADE; DROP PROCEDURE IF EXISTS "wrong_space".drop_proc"#).await.unwrap();
        assert_eq!(
            owner
                .query_one(query, &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "7"
        );
        for source in [
            r#"DROP FUNCTION "routine_space".drop_fn, "routine_space".missing_fn"#,
            r#"DROP PROCEDURE "routine_space".drop_proc, "routine_space".missing_proc"#,
        ] {
            assert!(owner.batch_execute(source).await.is_err());
            assert_eq!(
                owner
                    .query_one(query, &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "7"
            );
        }
        let drop_batch = drops.join("; ");
        owner.batch_execute("BEGIN").await.unwrap();
        owner.batch_execute(&drop_batch).await.unwrap();
        owner.batch_execute("ROLLBACK").await.unwrap();
        assert_eq!(
            owner
                .query_one(query, &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "7"
        );
        let count = r#"SELECT COUNT(*) FROM "pg_catalog"."pg_proc" WHERE "proname" IN ('drop_fn','drop_proc')"#;
        assert_eq!(
            owner.query_one(count, &[]).await.unwrap().get::<_, i64>(0),
            2
        );
        owner.batch_execute(&drop_batch).await.unwrap();
        for source in drops {
            let source = source
                .replacen("DROP FUNCTION", "DROP FUNCTION IF EXISTS", 1)
                .replacen("DROP PROCEDURE", "DROP PROCEDURE IF EXISTS", 1);
            owner.batch_execute(&source).await.unwrap();
        }
        assert_eq!(
            owner.query_one(count, &[]).await.unwrap().get::<_, i64>(0),
            0
        );
        drop(owner);
        drop(other);
        owner_connection.await.unwrap();
        other_connection.await.unwrap();
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
                "CREATE FUNCTION day_start(p_date DATE) RETURN DATE IS result DATE:=TRUNC(p_date); BEGIN RETURN result; END;",
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
        let statement = client
            .prepare_typed(
                "SELECT day_start($1) FROM DUAL",
                &[tokio_postgres::types::Type::TIMESTAMP],
            )
            .await
            .unwrap();
        assert_eq!(
            *statement.columns()[0].type_(),
            tokio_postgres::types::Type::TIMESTAMP
        );
        for (input, expected) in [
            (1709251198, 1709164800),
            (1709337598, 1709251200),
            (1709251198, 1709164800),
        ] {
            let value = std::time::UNIX_EPOCH + std::time::Duration::from_secs(input);
            let row = client.query_one(&statement, &[&value]).await.unwrap();
            assert_eq!(
                row.get::<_, std::time::SystemTime>(0),
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(expected)
            );
        }
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

#[tokio::test]
async fn procedural_case_preserves_profile_types_errors_and_prepared_calls_after_reopen() {
    use tokio_postgres::types::Type;
    for profile in ["postgres", "oracle-preview-v1"] {
        let directory = tempfile::tempdir().unwrap();
        for attempt in 0..2 {
            let server = Running::with_profile(directory.path(), profile);
            let mut config = tokio_postgres::Config::new();
            config
                .host("127.0.0.1")
                .port(server.port)
                .user("bicdb")
                .dbname("bicdb")
                .options(&format!("-c bicdb.sql_profile={profile}"));
            let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
            let connection = tokio::spawn(async move {
                connection.await.unwrap();
            });
            if attempt == 0 {
                let bodies = [
                    ("case_route", "BEGIN CASE WHEN p_value=1 THEN RETURN 10; WHEN p_value=0 THEN RETURN 20; ELSE CASE p_value WHEN 2 THEN RETURN 30; ELSE RETURN 40; END CASE; END CASE; END"),
                    ("case_missing", "BEGIN CASE WHEN p_value=1 THEN RETURN 10; END CASE; RETURN 20; END"),
                    ("case_handled", "BEGIN CASE WHEN p_value=1 THEN RETURN 10; END CASE; RETURN 20; EXCEPTION WHEN CASE_NOT_FOUND THEN RETURN 99; END"),
                    ("case_division", "BEGIN CASE WHEN p_value=1 THEN RETURN 10; WHEN 1/p_value>0 THEN RETURN 20; ELSE RETURN 30; END CASE; END"),
                ];
                for (name, body) in bodies {
                    let sql = if profile == "postgres" {
                        format!("CREATE FUNCTION {name}(p_value INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $${body}$$")
                    } else {
                        format!("CREATE FUNCTION {name}(p_value NUMBER) RETURN NUMBER IS {body};")
                    };
                    client.batch_execute(&sql).await.unwrap();
                }
            }
            let statement = client
                .prepare("SELECT CAST(case_route(CAST($1 AS INTEGER)) AS VARCHAR(20)) AS chosen")
                .await
                .unwrap();
            assert_eq!(statement.params(), &[Type::INT4]);
            assert_eq!(statement.columns()[0].type_(), &Type::VARCHAR);
            for (value, expected) in [
                (Some(1i32), "10"),
                (Some(0), "20"),
                (Some(2), "30"),
                (None, "40"),
            ] {
                assert_eq!(
                    client
                        .query_one(&statement, &[&value])
                        .await
                        .unwrap()
                        .get::<_, String>(0),
                    expected
                );
            }
            let row = client
                .query_one("SELECT CAST(case_division(2) AS VARCHAR(20))", &[])
                .await
                .unwrap();
            assert_eq!(
                row.get::<_, String>(0),
                if profile == "postgres" { "30" } else { "20" }
            );
            let error = client
                .query("SELECT case_missing(0)", &[])
                .await
                .unwrap_err();
            assert_eq!(error.code().unwrap().code(), "20000");
            assert_eq!(
                client
                    .query_one("SELECT CAST(case_handled(0) AS VARCHAR(20))", &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "99"
            );
            assert_eq!(
                client
                    .query_one(&statement, &[&Some(1i32)])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "10"
            );
            drop(statement);
            drop(client);
            connection.await.unwrap();
        }
    }
}

#[tokio::test]
async fn character_functions_preserve_prepared_pg_transport_and_reopen_contracts() {
    use tokio_postgres::types::Type;
    for profile in ["postgres", "oracle-preview-v1"] {
        let directory = tempfile::tempdir().unwrap();
        for attempt in 0..2 {
            let server = Running::with_profile(directory.path(), profile);
            let mut config = tokio_postgres::Config::new();
            config
                .host("127.0.0.1")
                .port(server.port)
                .user("bicdb")
                .dbname("bicdb")
                .options(&format!("-c bicdb.sql_profile={profile}"));
            let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
            let connection = tokio::spawn(async move {
                connection.await.unwrap();
            });
            let statement = client
                .prepare_typed("SELECT chr($1) AS value", &[Type::INT4])
                .await
                .unwrap();
            assert_eq!(statement.params(), &[Type::INT4]);
            assert_eq!(statement.columns()[0].type_(), &Type::TEXT);
            for (number, expected) in [
                (Some(65i32), Some("A".to_string())),
                (
                    Some(49835),
                    Some(if profile == "postgres" {
                        char::from_u32(49835).unwrap().to_string()
                    } else {
                        "«".into()
                    }),
                ),
                (None, None),
                (Some(65), Some("A".into())),
            ] {
                let row = client.query_one(&statement, &[&number]).await.unwrap();
                assert_eq!(row.get::<_, Option<String>>(0), expected);
            }
            let error = client.query(&statement, &[&Some(0i32)]).await.unwrap_err();
            assert_eq!(
                error.code().unwrap().code(),
                if profile == "postgres" {
                    "54000"
                } else {
                    "0A000"
                }
            );
            assert_eq!(
                client
                    .query_one(&statement, &[&Some(65i32)])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "A"
            );
            if profile == "oracle-preview-v1" {
                if attempt == 0 {
                    client.batch_execute("CREATE FUNCTION character_echo(n NUMBER) RETURN VARCHAR2 IS BEGIN RETURN TO_CHAR(CHR(n)); END;").await.unwrap();
                }
                let text = client
                    .prepare_typed("SELECT TO_CHAR(CHR($1)) AS value", &[Type::TEXT])
                    .await
                    .unwrap();
                assert_eq!(text.params(), &[Type::TEXT]);
                assert_eq!(text.columns()[0].type_(), &Type::TEXT);
                assert_eq!(
                    client
                        .query_one(&text, &[&"49835.9"])
                        .await
                        .unwrap()
                        .get::<_, String>(0),
                    "«"
                );
                assert_eq!(
                    client
                        .query(&text, &[&"invalid"])
                        .await
                        .unwrap_err()
                        .code()
                        .unwrap()
                        .code(),
                    "22P02"
                );
                assert_eq!(
                    client
                        .query_one(&text, &[&"65"])
                        .await
                        .unwrap()
                        .get::<_, String>(0),
                    "A"
                );
                let function = client
                    .prepare_typed("SELECT character_echo(CAST($1 AS NUMERIC))", &[Type::INT4])
                    .await
                    .unwrap();
                assert_eq!(function.columns()[0].type_(), &Type::VARCHAR);
                assert_eq!(
                    client
                        .query_one(&function, &[&49835i32])
                        .await
                        .unwrap()
                        .get::<_, String>(0),
                    "«"
                );
            }
            drop(statement);
            drop(client);
            connection.await.unwrap();
        }
    }
}

#[tokio::test]
async fn dynamic_sql_preserves_bound_text_describe_and_restart() {
    let root = tempfile::tempdir().unwrap();
    for attempt in 0..2 {
        let server = Running::with_profile(root.path(), "oracle-preview-v1");
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(server.port)
            .user("bicdb")
            .dbname("bicdb")
            .options("-c bicdb.sql_profile=oracle-preview-v1");
        let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let task = tokio::spawn(async move {
            connection.await.unwrap();
        });
        if attempt == 0 {
            for sql in ["CREATE TABLE dyn_wire_rows(id NUMBER PRIMARY KEY)",
                "CREATE PROCEDURE dyn_wire_write IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN EXECUTE IMMEDIATE 'INSERT INTO dyn_wire_rows VALUES(:1)' USING 1;COMMIT;END;",
                "CREATE FUNCTION dyn_wire_echo(p VARCHAR2) RETURN VARCHAR2 IS v VARCHAR2(200);BEGIN EXECUTE IMMEDIATE 'SELECT :value FROM DUAL' INTO v USING p;RETURN v;END;"] {
                client.batch_execute(sql).await.unwrap_or_else(|e|panic!("{sql}: {e}"));
            }
            let prepared = client.prepare("CALL dyn_wire_write()").await.unwrap();
            assert_eq!(
                client
                    .query_one(
                        "SELECT CAST(COUNT(*) AS VARCHAR2(20)) FROM dyn_wire_rows",
                        &[]
                    )
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "0"
            );
            client.execute(&prepared, &[]).await.unwrap();
        }
        let echo = client
            .prepare("SELECT CAST(dyn_wire_echo($1) AS VARCHAR2(200)) FROM DUAL")
            .await
            .unwrap();
        for value in [
            "O'Reilly",
            "O''Reilly",
            "x');DELETE FROM dyn_wire_rows;--",
            "ñΩ",
        ] {
            assert_eq!(
                client
                    .query_one(&echo, &[&value])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                value
            );
        }
        assert_eq!(
            client
                .query_one(&echo, &[&""])
                .await
                .unwrap()
                .get::<_, Option<String>>(0),
            None
        );
        assert_eq!(
            client
                .query_one(
                    "SELECT CAST(COUNT(*) AS VARCHAR2(20)) FROM dyn_wire_rows",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, String>(0),
            "1"
        );
        drop(client);
        task.await.unwrap();
    }
}

#[tokio::test]
async fn autonomous_transactions_preserve_parent_and_prepared_statement_boundaries() {
    let root = tempfile::tempdir().unwrap();
    for attempt in 0..2 {
        let server = Running::with_profile(root.path(), "oracle-preview-v1");
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(server.port)
            .user("bicdb")
            .dbname("bicdb")
            .options("-c bicdb.sql_profile=oracle-preview-v1");
        let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let task = tokio::spawn(async move {
            connection.await.unwrap();
        });
        let (observer, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
        let observe_task = tokio::spawn(async move {
            connection.await.unwrap();
        });
        if attempt == 0 {
            for ddl in [
                "CREATE TABLE auto_wire_log(id NUMBER PRIMARY KEY)",
                "CREATE PROCEDURE auto_wire_ok(p_id NUMBER) IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN INSERT INTO auto_wire_log VALUES(p_id);COMMIT;END;",
                "CREATE PROCEDURE auto_wire_err(p_id NUMBER) IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN INSERT INTO auto_wire_log VALUES(p_id);COMMIT;RAISE_APPLICATION_ERROR(-20101,'committed child failure');END;",
                "CREATE FUNCTION auto_wire_fn RETURN NUMBER IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN INSERT INTO auto_wire_log VALUES(3);COMMIT;RETURN 42;END;",
                "CREATE TABLE auto_wire_counter(n NUMBER)",
                "INSERT INTO auto_wire_counter VALUES(0)",
                "CREATE PROCEDURE auto_wire_increment IS PRAGMA AUTONOMOUS_TRANSACTION;BEGIN UPDATE auto_wire_counter SET n=n+1;COMMIT;END;",
            ] {client.batch_execute(ddl).await.unwrap_or_else(|e|panic!("{ddl}: {e}"));}
            let call = client.prepare("CALL auto_wire_ok(1)").await.unwrap();
            assert_eq!(
                observer
                    .query_one(
                        "SELECT CAST(COUNT(*) AS VARCHAR2(20)) FROM auto_wire_log",
                        &[]
                    )
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "0"
            );
            client.execute(&call, &[]).await.unwrap();
            let function = client
                .prepare("SELECT CAST(auto_wire_fn() AS VARCHAR2(20)) FROM DUAL")
                .await
                .unwrap();
            assert_eq!(
                observer
                    .query_one(
                        "SELECT CAST(COUNT(*) AS VARCHAR2(20)) FROM auto_wire_log",
                        &[]
                    )
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "1"
            );
            assert_eq!(
                client
                    .query_one(&function, &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "42"
            );
            let error = client
                .batch_execute("CALL auto_wire_err(2)")
                .await
                .unwrap_err();
            assert_eq!(error.code().unwrap().code(), "P0001");
            assert_eq!(
                error.as_db_error().unwrap().message(),
                "ORA-20101: committed child failure"
            );
            client
                .batch_execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .await
                .unwrap();
            client.batch_execute("CALL auto_wire_ok(6)").await.unwrap();
            for (connection, expected) in [(&client, "0"), (&observer, "1")] {
                assert_eq!(
                    connection
                        .query_one(
                            "SELECT CAST(COUNT(*) AS VARCHAR2(20)) FROM auto_wire_log WHERE id=6",
                            &[]
                        )
                        .await
                        .unwrap()
                        .get::<_, String>(0),
                    expected
                );
            }
            client.batch_execute("ROLLBACK").await.unwrap();
            client
                .batch_execute("DELETE FROM auto_wire_log WHERE id=6")
                .await
                .unwrap();
            client
                .batch_execute("BEGIN;INSERT INTO auto_wire_log VALUES(9)")
                .await
                .unwrap();
            client.batch_execute("CALL auto_wire_ok(4)").await.unwrap();
            client
                .batch_execute("CALL auto_wire_increment()")
                .await
                .unwrap();
            client
                .batch_execute("CALL auto_wire_increment()")
                .await
                .unwrap();
            assert_eq!(
                observer
                    .query_one("SELECT CAST(n AS VARCHAR2(20)) FROM auto_wire_counter", &[])
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "2"
            );
            assert_eq!(
                observer
                    .query_one(
                        "SELECT CAST(COUNT(*) AS VARCHAR2(20)) FROM auto_wire_log WHERE id=9",
                        &[]
                    )
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "0"
            );
            assert_eq!(
                observer
                    .query_one(
                        "SELECT CAST(COUNT(*) AS VARCHAR2(20)) FROM auto_wire_log WHERE id=4",
                        &[]
                    )
                    .await
                    .unwrap()
                    .get::<_, String>(0),
                "1"
            );
            client
                .batch_execute("ROLLBACK;BEGIN;INSERT INTO auto_wire_log VALUES(9)")
                .await
                .unwrap();
            let error = client
                .batch_execute("CALL auto_wire_err(5)")
                .await
                .unwrap_err();
            assert_eq!(error.code().unwrap().code(), "P0001");
            client.batch_execute("ROLLBACK").await.unwrap();
        }
        assert_eq!(
            observer
                .query_one(
                    "SELECT CAST(COUNT(*) AS VARCHAR2(20)) FROM auto_wire_log",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, String>(0),
            "5"
        );
        assert_eq!(
            observer
                .query_one(
                    "SELECT CAST(COUNT(*) AS VARCHAR2(20)) FROM auto_wire_log WHERE id=9",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, String>(0),
            "0"
        );
        drop(client);
        drop(observer);
        task.await.unwrap();
        observe_task.await.unwrap();
    }
}

#[tokio::test]
async fn oracle_session_context_uses_peer_metadata_and_refuses_client_forgery() {
    let root = tempfile::tempdir().unwrap();
    let running = Running::with_profile(root.path(), "oracle-preview-v1");
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(running.port)
        .user("bicdb")
        .dbname("bicdb")
        .options("-c bicdb.sql_profile=oracle-preview-v1 -c bicdb.connection_client_address=203.0.113.9 -c bicdb.connection_client_host=forged.example")
        .connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(async move {
        connection.await.unwrap();
    });
    let prepared = client
        .prepare("SELECT SYS_CONTEXT($1,$2) FROM DUAL")
        .await
        .unwrap();
    for command in ["RESET ALL", "DISCARD ALL"] {
        let row = client
            .query_one(&prepared, &[&"USERENV", &"IP_ADDRESS"])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "127.0.0.1");
        assert_eq!(
            row.columns()[0].type_(),
            &tokio_postgres::types::Type::VARCHAR
        );
        let host = client
            .query_one("SELECT SYS_CONTEXT('USERENV','HOST') FROM DUAL", &[])
            .await
            .unwrap();
        assert_eq!(host.get::<_, Option<String>>(0), None);
        for key in [
            "bicdb.connection_client_address",
            "bicdb.connection_client_host",
        ] {
            let error = client
                .execute(&format!("SELECT set_config('{key}','forged',false)"), &[])
                .await
                .unwrap_err();
            assert_eq!(error.code().unwrap().code(), "42501");
        }
        client.batch_execute(command).await.unwrap();
        // DISCARD drops prepared statements, so check the final value via a
        // fresh statement and leave the loop before reusing a discarded one.
        let row = client
            .query_one("SELECT SYS_CONTEXT('USERENV','IP_ADDRESS') FROM DUAL", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "127.0.0.1");
    }
    drop(client);
    connection.await.unwrap();
    drop(running);
}

#[tokio::test]
async fn oracle_anonymous_host_binds_keep_metadata_literals_and_repeat_execution() {
    use tokio_postgres::types::Type;
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let (client, connection) = transaction_defaults_connect(&server, "oracle-preview-v1").await;
    client
        .batch_execute("CREATE TABLE host_rows(key_value INTEGER PRIMARY KEY,payload TEXT)")
        .await
        .unwrap();
    let statement = client
        .prepare("BEGIN INSERT INTO host_rows VALUES($1,$2); END;")
        .await
        .unwrap();
    assert_eq!(statement.params(), &[Type::INT4, Type::TEXT]);
    assert!(statement.columns().is_empty());
    let count: i64 = client
        .query_one("SELECT CAST(COUNT(*) AS BIGINT) FROM host_rows", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0, "Parse and Describe must not execute the body");
    let text = "report $bicdb_oracle_0$; punctuation ? 'Привет'";
    client.execute(&statement, &[&7_i32, &text]).await.unwrap();
    let empty: Option<&str> = None;
    client.execute(&statement, &[&8_i32, &empty]).await.unwrap();
    let rows = client
        .query(
            "SELECT key_value,payload FROM host_rows ORDER BY key_value",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 7);
    assert_eq!(rows[0].get::<_, String>(1), text);
    assert_eq!(rows[1].get::<_, Option<String>>(1), None);
    let quoted = client
        .prepare("BEGIN INSERT INTO host_rows VALUES($1,'literal $9'); -- $200 is a comment\nEND;")
        .await
        .unwrap();
    assert_eq!(quoted.params(), &[Type::INT4]);
    client.execute(&quoted, &[&9_i32]).await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT payload FROM host_rows WHERE key_value=9", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "literal $9"
    );
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn oracle_anonymous_host_binds_resolve_declaration_defaults_and_assignments() {
    use tokio_postgres::types::Type;
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let (client, connection) = transaction_defaults_connect(&server, "oracle-preview-v1").await;
    client
        .batch_execute("CREATE TABLE host_rows(key_value INTEGER PRIMARY KEY,payload TEXT)")
        .await
        .unwrap();
    for (sql,key) in [
        ("DECLARE item INTEGER := $1; BEGIN INSERT INTO host_rows VALUES(item,'default'); END;",1_i32),
        ("DECLARE item INTEGER; BEGIN item := $1; INSERT INTO host_rows VALUES(item,'assignment'); END;",2_i32),
    ] {
        let statement=client.prepare(sql).await.unwrap();
        assert_eq!(statement.params(), &[Type::INT4]);
        client.execute(&statement,&[&key]).await.unwrap();
    }
    assert_eq!(
        client
            .query("SELECT key_value FROM host_rows", &[])
            .await
            .unwrap()
            .len(),
        2
    );
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn oracle_anonymous_host_binds_preserve_caller_transactions_and_statement_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let (client, connection) = transaction_defaults_connect(&server, "oracle-preview-v1").await;
    client.batch_execute("CREATE TABLE host_rows(key_value INTEGER PRIMARY KEY,payload TEXT);INSERT INTO host_rows VALUES(1,'base');BEGIN;INSERT INTO host_rows VALUES(2,'prior')").await.unwrap();
    let statement=client.prepare("BEGIN INSERT INTO host_rows VALUES($1,'temporary'); INSERT INTO host_rows VALUES(1,'duplicate'); END;").await.unwrap();
    let error = client.execute(&statement, &[&3_i32]).await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "23505");
    let rows = client
        .query("SELECT key_value FROM host_rows ORDER BY key_value", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(),
        vec![1, 2]
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        client
            .query("SELECT key_value FROM host_rows", &[])
            .await
            .unwrap()
            .len(),
        1
    );
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn oracle_anonymous_host_binds_do_not_reinterpret_native_dollar_quoted_bodies() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let directory = tempfile::tempdir().unwrap();
        let server = Running::with_profile(directory.path(), profile);
        let (client, connection) = transaction_defaults_connect(&server, profile).await;
        let result = client
            .prepare("DO $$BEGIN PERFORM $1; END$$ LANGUAGE plpgsql")
            .await;
        if profile == "postgres" {
            assert!(result.unwrap().params().is_empty());
        } else {
            // The preview front end rejects explicit PostgreSQL DO source.
            // Its generated internal DO representation is a separate boundary.
            assert_eq!(result.unwrap_err().code().unwrap().code(), "42601");
        }
        drop(client);
        connection.await.unwrap();
    }
}

#[tokio::test]
async fn oracle_anonymous_host_error_policies_remain_independent() {
    for profile in ["postgres", "oracle-preview-v1"] {
        let directory = tempfile::tempdir().unwrap();
        let server = Running::with_profile(directory.path(), profile);
        let (client, connection) = transaction_defaults_connect(&server, profile).await;
        client.batch_execute("CREATE TABLE host_rows(key_value INTEGER PRIMARY KEY);INSERT INTO host_rows VALUES(1);BEGIN;INSERT INTO host_rows VALUES(2)").await.unwrap();
        let error = client
            .execute("INSERT INTO host_rows VALUES($1)", &[&1_i32])
            .await
            .unwrap_err();
        assert_eq!(error.code().unwrap().code(), "23505");
        if profile == "postgres" {
            let error = client
                .query("SELECT key_value FROM host_rows", &[])
                .await
                .unwrap_err();
            assert_eq!(error.code().unwrap().code(), "25P02");
        } else {
            let rows = client
                .query("SELECT key_value FROM host_rows ORDER BY key_value", &[])
                .await
                .unwrap();
            assert_eq!(
                rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(),
                vec![1, 2]
            );
            client
                .batch_execute("INSERT INTO host_rows VALUES(4)")
                .await
                .unwrap();
        }
        client.batch_execute("ROLLBACK").await.unwrap();
        assert_eq!(
            client
                .query("SELECT key_value FROM host_rows", &[])
                .await
                .unwrap()
                .len(),
            1
        );
        drop(client);
        connection.await.unwrap();
    }
}

#[tokio::test]
async fn oracle_anonymous_host_commit_boundaries_preserve_durable_work() {
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let (client, connection) = transaction_defaults_connect(&server, "oracle-preview-v1").await;
    client.batch_execute("CREATE TABLE host_rows(key_value INTEGER PRIMARY KEY);INSERT INTO host_rows VALUES(1);BEGIN;INSERT INTO host_rows VALUES(2)").await.unwrap();
    let statement=client.prepare("BEGIN INSERT INTO host_rows VALUES($1); COMMIT; INSERT INTO host_rows VALUES(4); INSERT INTO host_rows VALUES(1); END;").await.unwrap();
    for (key, expected) in [(3_i32, vec![1, 2, 3]), (6_i32, vec![1, 2, 3, 6])] {
        let error = client.execute(&statement, &[&key]).await.unwrap_err();
        assert_eq!(error.code().unwrap().code(), "23505");
        let rows = client
            .query("SELECT key_value FROM host_rows ORDER BY key_value", &[])
            .await
            .unwrap();
        assert_eq!(
            rows.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>(),
            expected
        );
    }
    client
        .batch_execute("BEGIN;INSERT INTO host_rows VALUES(5);ROLLBACK")
        .await
        .unwrap();
    assert_eq!(
        client
            .query("SELECT key_value FROM host_rows", &[])
            .await
            .unwrap()
            .len(),
        4
    );
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn persistent_blob_storage_keeps_pg_binary_transport_and_transaction_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let (client, connection) = transaction_defaults_connect(&server, "oracle-preview-v1").await;
    client.batch_execute("CREATE TABLE blob_transport(id NUMBER PRIMARY KEY,b BLOB,n NUMBER); INSERT INTO blob_transport VALUES(1,HEXTORAW('00FF'),0)").await.unwrap();
    let prepared = client
        .prepare("SELECT b FROM blob_transport WHERE id=1")
        .await
        .unwrap();
    assert_eq!(
        prepared.columns()[0].type_(),
        &tokio_postgres::types::Type::BYTEA
    );
    assert!(prepared.columns()[0].table_oid().is_some());
    assert_eq!(prepared.columns()[0].column_id(), Some(2));
    assert_eq!(
        client
            .query_one(&prepared, &[])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        vec![0, 255]
    );
    client.batch_execute("BEGIN; UPDATE blob_transport SET b=HEXTORAW('AB') WHERE id=1; SAVEPOINT retained; UPDATE blob_transport SET id=3 WHERE id=1; ROLLBACK TO SAVEPOINT retained").await.unwrap();
    assert_eq!(
        client
            .query_one(&prepared, &[])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        vec![0xab]
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        client
            .query_one(&prepared, &[])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        vec![0, 255]
    );
    client.batch_execute("INSERT INTO blob_transport SELECT 2,b,n FROM blob_transport WHERE id=1; DELETE FROM blob_transport WHERE id=1").await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT b FROM blob_transport WHERE id=2", &[])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        vec![0, 255]
    );
    for name in ["__bicdb_oracle_lob_owners", "__bicdb_sql_compatibility"] {
        assert!(client
            .simple_query(&format!("SELECT * FROM \"{name}\""))
            .await
            .is_err());
    }
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn postgres_quoted_column_origin_metadata_preserves_distinct_case() {
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "postgres");
    let (client, connection) = transaction_defaults_connect(&server, "postgres").await;
    client.batch_execute(r#"CREATE TABLE "OriginCase"("V" BYTEA,"v" BYTEA); INSERT INTO "OriginCase" VALUES('\xab'::bytea,'\xcd'::bytea)"#).await.unwrap();
    let query = client
        .prepare(r#"SELECT "Q"."V","Q"."v" FROM "OriginCase" AS "Q""#)
        .await
        .unwrap();
    assert!(query.columns()[0].table_oid().is_some());
    assert_eq!(
        query.columns()[0].table_oid(),
        query.columns()[1].table_oid()
    );
    assert_eq!(query.columns()[0].column_id(), Some(1));
    assert_eq!(query.columns()[1].column_id(), Some(2));
    let row = client.query_one(&query, &[]).await.unwrap();
    assert_eq!(row.get::<_, Vec<u8>>(0), vec![0xab]);
    assert_eq!(row.get::<_, Vec<u8>>(1), vec![0xcd]);
    drop(client);
    connection.await.unwrap();
}

#[tokio::test]
async fn oracle_chunked_persistent_blob_append_roundtrips_over_pg_driver_and_rolls_back() {
    let directory = tempfile::tempdir().unwrap();
    let server = Running::with_profile(directory.path(), "oracle-preview-v1");
    let (client, connection) = transaction_defaults_connect(&server, "oracle-preview-v1").await;
    client.batch_execute("CREATE TABLE blob_streams(id NUMBER PRIMARY KEY,b BLOB); INSERT INTO blob_streams VALUES(1,HEXTORAW('AB'))").await.unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    let expected = (0..8193usize)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let mut block=String::from("DECLARE d BLOB; BEGIN UPDATE blob_streams SET b=EMPTY_BLOB() WHERE id=1; SELECT b INTO d FROM blob_streams WHERE id=1 FOR UPDATE;");
    for chunk in expected.chunks(1024) {
        let hex = chunk
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        block.push_str(&format!("DBMS_LOB.APPEND(d,HEXTORAW('{hex}'));"));
    }
    block.push_str("END;");
    let statement = client.prepare(&block).await.unwrap();
    client.execute(&statement, &[]).await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT b FROM blob_streams", &[])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        expected
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT b FROM blob_streams", &[])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        vec![0xab]
    );
    client.batch_execute("CREATE FUNCTION read_stream RETURN BLOB IS d BLOB; BEGIN SELECT b INTO d FROM blob_streams WHERE id=1; RETURN d; END;").await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT read_stream() FROM DUAL", &[])
            .await
            .unwrap()
            .get::<_, Vec<u8>>(0),
        vec![0xab]
    );
    drop(client);
    connection.await.unwrap();
}
