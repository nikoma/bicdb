use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_pgwire::{PgWireConfig, PgWireServer};
use std::{net::TcpListener, thread, time::Duration};
use tokio_postgres::{Client, NoTls};

struct Scenario {
    name: &'static str,
    reads: [&'static str; 2],
    writes: [&'static str; 2],
    requires_abort: bool,
    valid_final_rows: &'static [&'static [(i32, i32)]],
}

async fn outcome(client: &Client, sql: &str) -> Option<String> {
    match client.batch_execute(sql).await {
        Ok(()) => None,
        Err(error) => Some(error.code().expect("server SQLSTATE").code().to_owned()),
    }
}

#[tokio::test]
async fn serializable_tracks_row_and_predicate_dependencies() {
    let scenarios = [
        Scenario {
            name: "disjoint point reads and writes",
            reads: [
                "SELECT v FROM ssi_scope WHERE id=1",
                "SELECT v FROM ssi_scope WHERE id=2",
            ],
            writes: [
                "UPDATE ssi_scope SET v=11 WHERE id=1",
                "UPDATE ssi_scope SET v=21 WHERE id=2",
            ],
            requires_abort: false,
            valid_final_rows: &[&[(1, 11), (2, 21)]],
        },
        Scenario {
            name: "one read-write dependency",
            reads: [
                "SELECT v FROM ssi_scope WHERE id=1",
                "SELECT v FROM ssi_scope WHERE id=1",
            ],
            writes: [
                "UPDATE ssi_scope SET v=21 WHERE id=2",
                "UPDATE ssi_scope SET v=11 WHERE id=1",
            ],
            requires_abort: false,
            valid_final_rows: &[&[(1, 11), (2, 21)]],
        },
        Scenario {
            name: "write skew",
            reads: [
                "SELECT v FROM ssi_scope WHERE id=2",
                "SELECT v FROM ssi_scope WHERE id=1",
            ],
            writes: [
                "UPDATE ssi_scope SET v=11 WHERE id=1",
                "UPDATE ssi_scope SET v=21 WHERE id=2",
            ],
            requires_abort: true,
            valid_final_rows: &[&[(1, 11), (2, 20)], &[(1, 10), (2, 21)]],
        },
        Scenario {
            name: "phantom in a previously empty predicate",
            reads: [
                "SELECT v FROM ssi_scope WHERE id>10",
                "SELECT v FROM ssi_scope WHERE id=1",
            ],
            writes: [
                "UPDATE ssi_scope SET v=11 WHERE id=1",
                "INSERT INTO ssi_scope VALUES(11,110)",
            ],
            requires_abort: true,
            valid_final_rows: &[&[(1, 11), (2, 20)], &[(1, 10), (2, 20), (11, 110)]],
        },
    ];
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        drop(
            BicDb::open_with_config(
                root.path(),
                DbConfig::default().with_storage_mode(mode.clone()),
            )
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = PgWireServer::open(root.path(), PgWireConfig::default()).unwrap();
        let serving = server.clone();
        let serving_task =
            thread::spawn(move || bicdb_pgwire::serve_existing_listener(serving, listener));
        let config = format!("host=127.0.0.1 port={port} user=bicdb dbname=bicdb");
        let (a, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let a_task = tokio::spawn(connection);
        let (b, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let b_task = tokio::spawn(connection);
        let (peer, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        let peer_task = tokio::spawn(connection);
        peer.batch_execute("CREATE TABLE ssi_scope(id INT PRIMARY KEY, v INT NOT NULL)")
            .await
            .unwrap();
        a.batch_execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .await
            .unwrap();
        for case in &scenarios {
            peer.batch_execute("DELETE FROM ssi_scope; INSERT INTO ssi_scope VALUES (1,10),(2,20)")
                .await
                .unwrap();
            a.batch_execute("BEGIN").await.unwrap();
            b.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
                .await
                .unwrap();
            a.batch_execute(case.reads[0]).await.unwrap();
            b.batch_execute(case.reads[1]).await.unwrap();
            let a_write = outcome(&a, case.writes[0]).await;
            let b_write = outcome(&b, case.writes[1]).await;
            let a_end = outcome(
                &a,
                if a_write.is_some() {
                    "ROLLBACK"
                } else {
                    "COMMIT"
                },
            )
            .await;
            let b_end = outcome(
                &b,
                if b_write.is_some() {
                    "ROLLBACK"
                } else {
                    "COMMIT"
                },
            )
            .await;
            let errors = [a_write, b_write, a_end, b_end]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            let expected = if case.requires_abort {
                &["40001"][..]
            } else {
                &[]
            };
            assert_eq!(errors, expected, "{mode:?}: {}", case.name);
            let rows = peer
                .query("SELECT id,v FROM ssi_scope ORDER BY id", &[])
                .await
                .unwrap()
                .into_iter()
                .map(|row| (row.get::<_, i32>(0), row.get::<_, i32>(1)))
                .collect::<Vec<_>>();
            assert!(
                case.valid_final_rows.contains(&rows.as_slice()),
                "{mode:?}: {} committed {rows:?}",
                case.name
            );
        }

        a.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY")
            .await
            .unwrap();
        let before = a
            .query_one("SELECT v FROM ssi_scope WHERE id=1", &[])
            .await
            .unwrap()
            .get::<_, i32>(0);
        b.batch_execute(
            "BEGIN ISOLATION LEVEL SERIALIZABLE; UPDATE ssi_scope SET v=100 WHERE id=1; COMMIT",
        )
        .await
        .unwrap();
        let during = a
            .query_one("SELECT v FROM ssi_scope WHERE id=1", &[])
            .await
            .unwrap()
            .get::<_, i32>(0);
        assert_eq!(during, before, "{mode:?}: read-only snapshot moved");
        a.batch_execute("COMMIT").await.unwrap();

        peer.batch_execute("UPDATE ssi_scope SET v=10 WHERE id=1")
            .await
            .unwrap();
        b.batch_execute(
            "BEGIN ISOLATION LEVEL SERIALIZABLE; UPDATE ssi_scope SET v=200 WHERE id=2",
        )
        .await
        .unwrap();
        a.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
            .await
            .unwrap();
        let sql = "SELECT v FROM ssi_scope WHERE id=1";
        {
            let read = a.query_one(sql, &[]);
            tokio::pin!(read);
            assert!(
                tokio::time::timeout(Duration::from_millis(200), &mut read)
                    .await
                    .is_err(),
                "{mode:?}: deferrable read completed while its overlapping writer was active"
            );
            b.batch_execute("COMMIT").await.unwrap();
            let value = tokio::time::timeout(Duration::from_secs(5), &mut read)
                .await
                .expect("deferrable read did not resume after the writer committed")
                .unwrap()
                .get::<_, i32>(0);
            assert_eq!(value, 10, "{mode:?}: initial deferrable snapshot");
        }
        b.batch_execute(
            "BEGIN ISOLATION LEVEL SERIALIZABLE; UPDATE ssi_scope SET v=11 WHERE id=1; COMMIT",
        )
        .await
        .unwrap();
        let value = a.query_one(sql, &[]).await.unwrap().get::<_, i32>(0);
        assert_eq!(value, 10, "{mode:?}: deferrable snapshot moved");
        assert_eq!(
            outcome(&a, "INSERT INTO ssi_scope VALUES (99,99)")
                .await
                .as_deref(),
            Some("25006"),
            "{mode:?}: deferrable read-only transaction permitted a write"
        );
        a.batch_execute("ROLLBACK").await.unwrap();

        peer.batch_execute(
            "CREATE INDEX ssi_scope_value ON ssi_scope(v); UPDATE ssi_scope SET v=10 WHERE id=1",
        )
        .await
        .unwrap();
        a.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY; SELECT 1")
            .await
            .unwrap();
        b.batch_execute(
            "BEGIN ISOLATION LEVEL SERIALIZABLE; UPDATE ssi_scope SET v=11 WHERE id=1; COMMIT",
        )
        .await
        .unwrap();
        for (predicate, expected) in [
            ("v=10", &[1][..]),
            ("v=11", &[][..]),
            ("v BETWEEN 9 AND 10", &[1][..]),
            ("v BETWEEN 11 AND 12", &[][..]),
        ] {
            let query = format!("SELECT id FROM ssi_scope WHERE {predicate} ORDER BY id");
            let rows = a
                .query(&query, &[])
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.get::<_, i32>(0))
                .collect::<Vec<_>>();
            assert_eq!(
                rows, expected,
                "{mode:?}: secondary index predicate {predicate} disagrees with the fixed snapshot"
            );
        }
        a.batch_execute("COMMIT").await.unwrap();

        peer.batch_execute(
            "CREATE TABLE ssi_ranked(id INT PRIMARY KEY, grp INT, v INT);
             CREATE INDEX ssi_ranked_order ON ssi_ranked(grp,v);
             INSERT INTO ssi_ranked VALUES (1,1,10),(2,1,20),(3,2,0)",
        )
        .await
        .unwrap();
        a.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY; SELECT 1")
            .await
            .unwrap();
        peer.batch_execute("UPDATE ssi_ranked SET v=30 WHERE id=1")
            .await
            .unwrap();
        let row = a
            .query_one(
                "SELECT id FROM ssi_ranked WHERE grp=1 ORDER BY v ASC LIMIT 1",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            row.get::<_, i32>(0),
            1,
            "{mode:?}: bounded order lost its historical winner"
        );
        a.batch_execute("COMMIT").await.unwrap();

        peer.batch_execute(
            r#"CREATE TABLE ssi_documents(id INT PRIMARY KEY, body JSONB);
               CREATE INDEX ssi_documents_body ON ssi_documents USING gin(body);
               INSERT INTO ssi_documents VALUES (1,'{"tag":"old"}')"#,
        )
        .await
        .unwrap();
        a.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY; SELECT 1")
            .await
            .unwrap();
        peer.batch_execute(r#"UPDATE ssi_documents SET body='{"tag":"new"}' WHERE id=1"#)
            .await
            .unwrap();
        let row = a
            .query_one(
                r#"SELECT id FROM ssi_documents WHERE body @> '{"tag":"old"}'::jsonb"#,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            row.get::<_, i32>(0),
            1,
            "{mode:?}: current-only GIN postings hid a historical row"
        );
        a.batch_execute("COMMIT").await.unwrap();

        b.batch_execute(
            "BEGIN ISOLATION LEVEL SERIALIZABLE; UPDATE ssi_scope SET v=201 WHERE id=2",
        )
        .await
        .unwrap();
        a.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
            .await
            .unwrap();
        {
            let read = a.simple_query("SELECT v FROM ssi_scope WHERE id=1");
            tokio::pin!(read);
            assert!(tokio::time::timeout(Duration::from_millis(200), &mut read)
                .await
                .is_err());
            a.cancel_token().cancel_query(NoTls).await.unwrap();
            let error = tokio::time::timeout(Duration::from_secs(5), &mut read)
                .await
                .expect("deferrable simple query did not respond to cancellation")
                .unwrap_err();
            assert_eq!(error.code().unwrap().code(), "57014");
        }
        a.batch_execute("ROLLBACK").await.unwrap();
        b.batch_execute("COMMIT").await.unwrap();

        b.batch_execute("BEGIN; INSERT INTO ssi_scope VALUES (100,10)")
            .await
            .unwrap();
        {
            let update = a.batch_execute(
                "INSERT INTO ssi_scope VALUES (100,20) ON CONFLICT (id) DO UPDATE SET v=excluded.v",
            );
            tokio::pin!(update);
            assert!(
                tokio::time::timeout(Duration::from_millis(200), &mut update)
                    .await
                    .is_err(),
                "{mode:?}: implicit Serializable upsert did not wait for the conflicting insert"
            );
            b.batch_execute("COMMIT").await.unwrap();
            let error = tokio::time::timeout(Duration::from_secs(5), &mut update)
                .await
                .expect("implicit Serializable upsert did not finish after the owner committed")
                .unwrap_err();
            assert_eq!(error.code().unwrap().code(), "40001");
        }
        let value = a
            .query_one("SELECT v FROM ssi_scope WHERE id=100", &[])
            .await
            .unwrap()
            .get::<_, i32>(0);
        assert_eq!(value, 10, "{mode:?}: implicit upsert changed the winner");

        drop((a, b, peer));
        a_task.abort();
        b_task.abort();
        peer_task.abort();
        server.request_shutdown();
        serving_task.join().unwrap().unwrap();
    }
}
