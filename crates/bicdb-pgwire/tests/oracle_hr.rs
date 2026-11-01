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
        let config = PgWireConfig {
            sql_profile: if oracle { "oracle-hr-v1" } else { "postgres" }.into(),
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
