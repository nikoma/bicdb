//! Migration-tool protocol coverage for DECLARE, multidimensional arrays,
//! FOREACH SLICE, nested IF, dynamic DDL, and repeated compatibility renames.
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, sync::Arc, thread};

struct Server {
    server: Arc<PgWireServer>,
    thread: Option<thread::JoinHandle<bicdb_pgwire::Result<()>>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.server.request_shutdown();
        self.thread.take().unwrap().join().unwrap().unwrap();
    }
}

#[tokio::test]
async fn declared_array_slice_migration_runs_twice_through_pgwire() {
    let directory = tempfile::tempdir().unwrap();
    let server = PgWireServer::open(directory.path(), PgWireConfig::default()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let copy = server.clone();
    let _guard = Server {
        server,
        thread: Some(thread::spawn(move || {
            bicdb_pgwire::serve_existing_listener(copy, listener)
        })),
    };
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("bicdb")
        .dbname("bicdb")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let task = tokio::spawn(connection);
    client.batch_execute("CREATE TABLE compactitems (id INTEGER PRIMARY KEY, label TEXT); CREATE TABLE compactgroups (id INTEGER PRIMARY KEY, label TEXT); INSERT INTO compactitems VALUES (1, 'kept'); INSERT INTO compactgroups VALUES (2, 'also kept');").await.unwrap();
    let migration = r#"
BEGIN;
DO $compatibility_renames$
DECLARE
    relation_pair TEXT[];
    relation_pairs CONSTANT TEXT[][] := ARRAY[
        ARRAY['compactitems', 'compact_items'],
        ARRAY['compactgroups', 'compact_groups']
    ];
BEGIN
    FOREACH relation_pair SLICE 1 IN ARRAY relation_pairs LOOP
        IF to_regclass('public.' || relation_pair[2]) IS NULL THEN
            IF to_regclass('public.' || relation_pair[1]) IS NULL THEN
                RAISE EXCEPTION 'missing relation';
            END IF;
            EXECUTE format('ALTER TABLE public.%I RENAME TO %I', relation_pair[1], relation_pair[2]);
        END IF;
        IF to_regclass('public.' || relation_pair[1]) IS NULL THEN
            EXECUTE format('CREATE VIEW public.%I WITH (security_invoker = true) AS SELECT * FROM public.%I', relation_pair[1], relation_pair[2]);
        END IF;
    END LOOP;
END
$compatibility_renames$;
COMMIT;
"#;
    for _ in 0..2 {
        client.batch_execute(migration).await.unwrap();
    }
    for (old, new, expected) in [
        ("compactitems", "compact_items", "kept"),
        ("compactgroups", "compact_groups", "also kept"),
    ] {
        for name in [old, new] {
            let row = client
                .query_one(&format!("SELECT label FROM {name}"), &[])
                .await
                .unwrap();
            assert_eq!(row.get::<_, String>(0), expected);
        }
    }
    drop(client);
    task.await.unwrap().unwrap();
}
