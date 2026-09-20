use std::env;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::thread;

use bicdb_pgwire::{PgWireConfig, PgWireServer};
use tokio::task::JoinHandle;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

struct RunningBicDb {
    address: SocketAddr,
    server: std::sync::Arc<PgWireServer>,
    thread: thread::JoinHandle<bicdb_pgwire::Result<()>>,
}

impl RunningBicDb {
    fn start(path: &Path) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = PgWireServer::open(path, PgWireConfig::default()).unwrap();
        let serving = server.clone();
        let thread =
            thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        Self {
            address,
            server,
            thread,
        }
    }

    fn config(&self) -> String {
        format!(
            "host=127.0.0.1 port={} user=bicdb dbname=bicdb",
            self.address.port()
        )
    }

    fn stop(self) {
        self.server.request_shutdown();
        self.thread.join().unwrap().unwrap();
    }
}

async fn connect(config: &str) -> (Client, JoinHandle<()>) {
    let (client, connection) = tokio_postgres::connect(config, NoTls).await.unwrap();
    let task = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, task)
}

async fn simple_rows(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn assert_same_rows(postgres: &Client, bicdb: &Client, sql: &str) {
    assert_eq!(
        simple_rows(bicdb, sql).await,
        simple_rows(postgres, sql).await,
        "differential rows for {sql}"
    );
}

async fn apply_setup(client: &Client) {
    for statement in bicdb_sql::split_sql_statements(SETUP_SQL) {
        client.batch_execute(&statement).await.unwrap();
    }
}

fn sqlstate(error: &tokio_postgres::Error) -> Option<&str> {
    error.code().map(|code| code.code())
}

async fn enum_and_failed_transaction_gate(client: &Client) -> (String, String) {
    client.batch_execute("BEGIN").await.unwrap();
    let invalid = client
        .execute(
            "INSERT INTO cognee_diff_enum VALUES ('invalid', 'UNKNOWN')",
            &[],
        )
        .await
        .unwrap_err();
    let failed = client.simple_query("SELECT 1").await.unwrap_err();
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        simple_rows(client, "SELECT 1").await[0][0].as_deref(),
        Some("1")
    );
    (
        sqlstate(&invalid).unwrap_or_default().to_string(),
        sqlstate(&failed).unwrap_or_default().to_string(),
    )
}

async fn advisory_lock_gate(first: &Client, second: &Client) {
    let statement = first.prepare("SELECT pg_advisory_lock($1)").await.unwrap();
    assert_eq!(statement.params(), &[Type::INT8]);
    assert_eq!(statement.columns()[0].type_(), &Type::VOID);
    first.query(&statement, &[&9_001_i64]).await.unwrap();
    assert!(!second
        .query_one("SELECT pg_try_advisory_lock(9001::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(first
        .query_one("SELECT pg_advisory_unlock(9001::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    assert!(second
        .query_one("SELECT pg_try_advisory_lock(9001::bigint)", &[])
        .await
        .unwrap()
        .get::<_, bool>(0));
    second
        .query_one("SELECT pg_advisory_unlock(9001::bigint)", &[])
        .await
        .unwrap();
}

async fn prepared_contract_gate(client: &Client) -> (u32, u32) {
    let arrays = client
        .prepare("SELECT $1::text[], array_append($1::text[], $2::text)")
        .await
        .unwrap();
    assert_eq!(arrays.params(), &[Type::TEXT_ARRAY, Type::TEXT]);
    assert_eq!(arrays.columns()[0].type_(), &Type::TEXT_ARRAY);
    assert_eq!(arrays.columns()[1].type_(), &Type::TEXT_ARRAY);

    let timestamp = client.prepare("SELECT $1::timestamptz").await.unwrap();
    assert_eq!(timestamp.params(), &[Type::TIMESTAMPTZ]);
    assert_eq!(timestamp.columns()[0].type_(), &Type::TIMESTAMPTZ);

    let having = client
        .prepare(
            "WITH matching_neighbors AS (
                SELECT nbr_id AS id
                FROM (SELECT primary_id, nbr_id FROM cognee_diff_edges) sub
                GROUP BY nbr_id
                HAVING COUNT(DISTINCT lower(primary_id)) = $1
             )
             SELECT lhs.id
             FROM matching_neighbors lhs
             JOIN matching_neighbors rhs ON rhs.id = lhs.id
             ORDER BY lhs.id",
        )
        .await
        .unwrap();
    assert_eq!(having.params(), &[Type::INT8]);
    assert_eq!(having.columns()[0].type_(), &Type::TEXT);
    let rows = client.query(&having, &[&2_i64]).await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>(),
        vec!["n1".to_string()]
    );

    let vector = client
        .prepare(
            "SELECT embedding <=> $1 AS distance
             FROM \"CogneeDiffVector\"
             ORDER BY distance",
        )
        .await
        .unwrap();
    assert_eq!(vector.params()[0].name(), "vector");
    assert_eq!(vector.columns()[0].type_(), &Type::FLOAT8);

    let enum_oid = client
        .query_one(
            "SELECT oid::int4 FROM pg_type WHERE typname = 'cognee_diff_status'",
            &[],
        )
        .await
        .unwrap()
        .get::<_, i32>(0) as u32;
    let vector_oid = client
        .query_one(
            "SELECT oid::int4 FROM pg_type WHERE typname = 'vector'",
            &[],
        )
        .await
        .unwrap()
        .get::<_, i32>(0) as u32;
    assert_eq!(vector.params()[0].oid(), vector_oid);
    (enum_oid, vector_oid)
}

const SETUP_SQL: &str = r#"
CREATE EXTENSION IF NOT EXISTS vector;
DROP TABLE IF EXISTS cognee_diff_enum;
DROP TABLE IF EXISTS cognee_diff_edges;
DROP TABLE IF EXISTS cognee_diff_json;
DROP TABLE IF EXISTS "CogneeDiffVector";
DROP TABLE IF EXISTS "CogneeDiffQuoted";
DROP TYPE IF EXISTS cognee_diff_status;
CREATE TYPE cognee_diff_status AS ENUM ('STARTED', 'COMPLETED', 'FAILED');
CREATE TABLE cognee_diff_enum (
    id text PRIMARY KEY,
    status cognee_diff_status NOT NULL
);
CREATE TABLE cognee_diff_edges (
    edge_id text PRIMARY KEY,
    primary_id text NOT NULL,
    nbr_id text NOT NULL,
    provenance text[] NOT NULL DEFAULT '{}',
    observed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE cognee_diff_json (id text PRIMARY KEY, payload jsonb NOT NULL);
CREATE TABLE "CogneeDiffVector" (
    id text PRIMARY KEY,
    embedding vector(3) NOT NULL
);
CREATE TABLE "CogneeDiffQuoted" (id integer PRIMARY KEY);
INSERT INTO cognee_diff_enum VALUES ('run', 'STARTED');
INSERT INTO cognee_diff_edges VALUES
    ('e1', 'p1', 'n1', ARRAY['source-a']::text[], now()),
    ('e2', 'p2', 'n1', ARRAY['source-b']::text[], now()),
    ('e3', 'p1', 'n2', ARRAY['source-a']::text[], now());
INSERT INTO cognee_diff_edges
    VALUES ('e1', 'p1', 'n1', ARRAY['ignored']::text[], now())
    ON CONFLICT (edge_id) DO UPDATE SET
        provenance = array_append(cognee_diff_edges.provenance, 'source-c');
INSERT INTO cognee_diff_json VALUES
    ('a', '{"belongs_to_set":["alpha","beta"]}'::jsonb),
    ('b', '{"belongs_to_set":["gamma"]}'::jsonb),
    ('c', '{}'::jsonb);
INSERT INTO "CogneeDiffVector" VALUES
    ('x', '[1,0,0]'), ('y', '[0.8,0.2,0]'), ('z', '[0,1,0]');
INSERT INTO "CogneeDiffQuoted" VALUES (7);
SET TIME ZONE 'UTC';
SET jit = off;
"#;

#[tokio::test]
#[ignore = "requires scripts/cognee-pg18-diff.sh PostgreSQL 18 + pgvector oracle"]
async fn cognee_postgres_18_pgvector_differential_fixture() {
    let postgres_config = env::var("COGNEE_PG18_URL")
        .expect("COGNEE_PG18_URL must point to PostgreSQL 18 with pgvector");
    let dir = tempfile::tempdir().unwrap();
    let bicdb_server = RunningBicDb::start(dir.path());

    let (postgres, postgres_task) = connect(&postgres_config).await;
    let (postgres_second, postgres_second_task) = connect(&postgres_config).await;
    let (bicdb, bicdb_task) = connect(&bicdb_server.config()).await;
    let (bicdb_second, bicdb_second_task) = connect(&bicdb_server.config()).await;

    let oracle_version = simple_rows(&postgres, "SHOW server_version").await;
    assert!(oracle_version[0][0]
        .as_deref()
        .is_some_and(|version| version.starts_with("18.4")));

    apply_setup(&postgres).await;
    apply_setup(&bicdb).await;

    advisory_lock_gate(&postgres, &postgres_second).await;
    advisory_lock_gate(&bicdb, &bicdb_second).await;

    let postgres_oids = prepared_contract_gate(&postgres).await;
    let bicdb_oids = prepared_contract_gate(&bicdb).await;
    assert_ne!(postgres_oids.0, 0);
    assert_ne!(postgres_oids.1, 0);
    assert_ne!(bicdb_oids.0, 0);
    assert_ne!(bicdb_oids.1, 0);

    for sql in [
        "SELECT current_setting('jit'), set_config('jit', 'off', false)",
        "SELECT status::text FROM cognee_diff_enum ORDER BY status",
        "SELECT provenance FROM cognee_diff_edges WHERE edge_id = 'e1'",
        "SELECT observed_at IS NOT NULL FROM cognee_diff_edges WHERE edge_id = 'e1'",
        "SELECT id FROM \"CogneeDiffQuoted\"",
        "SELECT id, embedding <=> '[1,0,0]'::vector,
                    embedding <-> '[1,0,0]'::vector,
                    embedding <#> '[1,0,0]'::vector,
                    embedding <+> '[1,0,0]'::vector
             FROM \"CogneeDiffVector\" ORDER BY embedding <=> '[1,0,0]'::vector, id",
        "SELECT id FROM cognee_diff_json
             WHERE payload ? 'belongs_to_set'
               AND EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements_text(payload -> 'belongs_to_set') value
                   WHERE value = ANY(ARRAY['beta']::text[])
               ) ORDER BY id",
        "SELECT t.typname, n.nspname, e.enumlabel, e.enumsortorder::text
             FROM pg_type t
             JOIN pg_namespace n ON n.oid = t.typnamespace
             JOIN pg_enum e ON e.enumtypid = t.oid
             WHERE t.typname = 'cognee_diff_status'
             ORDER BY e.enumsortorder",
        "SELECT format_type(a.atttypid, a.atttypmod), a.atttypmod::text
             FROM pg_attribute a
             WHERE a.attrelid = '\"CogneeDiffVector\"'::regclass
               AND a.attname = 'embedding'",
    ] {
        assert_same_rows(&postgres, &bicdb, sql).await;
    }

    let postgres_missing = postgres
        .simple_query("SELECT id FROM cogneediffquoted")
        .await
        .unwrap_err();
    let bicdb_missing = bicdb
        .simple_query("SELECT id FROM cogneediffquoted")
        .await
        .unwrap_err();
    assert_eq!(sqlstate(&postgres_missing), Some("42P01"));
    assert_eq!(sqlstate(&bicdb_missing), Some("42P01"));

    let postgres_errors = enum_and_failed_transaction_gate(&postgres).await;
    let bicdb_errors = enum_and_failed_transaction_gate(&bicdb).await;
    assert_eq!(postgres_errors, ("22P02".to_string(), "25P02".to_string()));
    assert_eq!(bicdb_errors, postgres_errors);

    drop(bicdb_second);
    drop(bicdb);
    bicdb_server.server.request_shutdown();
    bicdb_second_task.await.unwrap();
    bicdb_task.await.unwrap();
    bicdb_server.stop();

    let reopened = RunningBicDb::start(dir.path());
    let (reopened_client, reopened_task) = connect(&reopened.config()).await;
    let reopened_oids = prepared_contract_gate(&reopened_client).await;
    assert_eq!(reopened_oids, bicdb_oids);
    for sql in [
        "SELECT status::text FROM cognee_diff_enum ORDER BY status",
        "SELECT provenance FROM cognee_diff_edges WHERE edge_id = 'e1'",
        "SELECT id, embedding <=> '[1,0,0]'::vector
             FROM \"CogneeDiffVector\" ORDER BY embedding <=> '[1,0,0]'::vector, id",
    ] {
        assert_same_rows(&postgres, &reopened_client, sql).await;
    }

    drop(reopened_client);
    reopened.server.request_shutdown();
    reopened_task.await.unwrap();
    reopened.stop();
    drop(postgres_second);
    drop(postgres);
    postgres_second_task.abort();
    postgres_task.abort();
}
