//! Internal access escalation must survive NOTICE and user exception handlers.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, thread, time::Duration};

#[tokio::test]
async fn routine_ddl_escalates_without_entering_user_handler_or_replaying_writes() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = PgWireServer::open(
        root.path(),
        PgWireConfig {
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .unwrap();
    let serving = server.clone();
    let task = thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    client.batch_execute("CREATE TABLE escalation_rows(id integer PRIMARY KEY); CREATE TABLE escalation_target(id integer); CREATE FUNCTION escalation_probe() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN INSERT INTO escalation_rows VALUES(1); BEGIN RAISE NOTICE 'before DDL'; EXECUTE 'ALTER TABLE escalation_target ADD COLUMN extra integer'; EXCEPTION WHEN OTHERS THEN RAISE EXCEPTION 'user handler caught: %', SQLERRM; END; RETURN 7; END $$").await.unwrap();
    client
        .batch_execute("BEGIN; INSERT INTO escalation_rows VALUES(100)")
        .await
        .unwrap();
    // Extended execution inside an existing transaction used to turn the
    // internal access request into a caught SQL error, preventing escalation.
    let result = client.query_one("SELECT escalation_probe()", &[]).await;
    let checked = if result.is_ok() {
        let count = client
            .query_one("SELECT count(*) FROM escalation_rows", &[])
            .await
            .unwrap()
            .get::<_, i64>(0);
        client.batch_execute("COMMIT").await.unwrap();
        assert!(client
            .query("SELECT extra FROM escalation_target", &[])
            .await
            .unwrap()
            .is_empty());
        client.batch_execute("CREATE FUNCTION escalation_user_error() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'operation requires exclusive database access' USING ERRCODE='0A000'; EXCEPTION WHEN OTHERS THEN RETURN 9; END $$").await.unwrap();
        assert_eq!(
            client
                .query_one("SELECT escalation_user_error()", &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            9
        );
        client.batch_execute("CREATE SCHEMA adempiere; SET search_path=adempiere,public; CREATE TABLE wire_privilege_probe(description varchar(20)); INSERT INTO wire_privilege_probe VALUES('retained'); CREATE VIEW wire_privilege_probe_v AS SELECT description FROM wire_privilege_probe").await.unwrap();
        client
            .batch_execute(include_str!(
                "../../bicdb-sql/tests/fixtures/postgres/idempiere_altercolumn.sql"
            ))
            .await
            .unwrap();
        client.batch_execute("BEGIN").await.unwrap();
        client.query("SELECT altercolumn('wire_privilege_probe'::name,'description'::name,'varchar(40)'::name,NULL::varchar,NULL::varchar)", &[]).await.unwrap();
        assert_eq!(
            client
                .query_one("SELECT description FROM wire_privilege_probe_v", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "retained"
        );
        assert_eq!(client.query_one("SELECT count(*) FROM information_schema.role_table_grants WHERE table_schema='adempiere' AND table_name='wire_privilege_probe_v'", &[]).await.unwrap().get::<_, i64>(0),7);
        client.batch_execute("COMMIT").await.unwrap();
        client.batch_execute("CREATE TABLE wire_rule_request(tablename name,columnname name,datatype name,nullclause varchar,defaultclause varchar)").await.unwrap();
        let rule = client.prepare("CREATE RULE wire_sync AS ON INSERT TO wire_rule_request DO INSTEAD SELECT altercolumn(new.tablename,new.columnname,new.datatype,new.nullclause,new.defaultclause)").await.unwrap();
        assert!(rule.columns().is_empty());
        assert_eq!(
            client
                .query_one(
                    "SELECT count(*) FROM pg_rules WHERE rulename='wire_sync'",
                    &[]
                )
                .await
                .unwrap()
                .get::<_, i64>(0),
            0,
            "Describe must not create the rule"
        );
        client.execute(&rule, &[]).await.unwrap();
        client.batch_execute("BEGIN").await.unwrap();
        assert_eq!(client.execute("INSERT INTO wire_rule_request VALUES('wire_privilege_probe','description','varchar(41)',NULL,NULL)", &[]).await.unwrap(),0);
        assert_eq!(
            client
                .query_one("SELECT description FROM wire_privilege_probe_v", &[])
                .await
                .unwrap()
                .get::<_, String>(0),
            "retained"
        );
        client.batch_execute("ROLLBACK").await.unwrap();
        count
    } else {
        client.batch_execute("ROLLBACK").await.unwrap();
        0
    };
    drop(client);
    connection_task.abort();
    server.request_shutdown();
    task.join().unwrap().unwrap();
    assert_eq!(result.unwrap().get::<_, i32>(0), 7);
    assert_eq!(
        checked, 2,
        "preserve earlier transaction writes and execute the routine insert once"
    );
}
