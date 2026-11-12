use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

fn exercise(mut run: impl FnMut(&mut BicDb), expected: Vec<Vec<SqlValue>>) {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        {
            let mut sql = SqlSession::new(&mut db);
            sql.execute("CREATE TABLE p(k INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(k)")
                .unwrap();
            sql.execute("CREATE TABLE low PARTITION OF p FOR VALUES FROM(0) TO(10)")
                .unwrap();
            sql.execute("CREATE TABLE high PARTITION OF p FOR VALUES FROM(10) TO(20)")
                .unwrap();
            sql.execute("INSERT INTO p VALUES(1,'original-ack')")
                .unwrap();
        }
        run(&mut db);
        for reopen in 0..3 {
            let mut sql = SqlSession::new(&mut db);
            assert_eq!(
                sql.execute("SELECT k,payload FROM p ORDER BY k")
                    .unwrap()
                    .rows,
                expected,
                "parent reopen{reopen}"
            );
            let mut leaves = sql
                .execute("SELECT k,payload FROM low ORDER BY k")
                .unwrap()
                .rows;
            leaves.extend(
                sql.execute("SELECT k,payload FROM high ORDER BY k")
                    .unwrap()
                    .rows,
            );
            assert_eq!(leaves, expected, "physical leaves reopen{reopen}");
            drop(sql);
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
    }
}
fn row(k: i64, payload: &str) -> Vec<SqlValue> {
    vec![SqlValue::Int(k), SqlValue::String(payload.into())]
}

#[test]
fn parent_upsert_updates_actual_leaf_and_projects_accessed_parent() {
    exercise(
        |db| {
            assert_eq!(SqlSession::new(db).execute("INSERT INTO p VALUES(1,'updated-ack') ON CONFLICT(k) DO UPDATE SET payload=excluded.payload RETURNING p.k,p.payload").unwrap().rows,vec![row(1,"updated-ack")]);
        },
        vec![row(1, "updated-ack")],
    );
}

#[test]
fn parent_do_nothing_arbitrates_each_leaf_and_keeps_nonconflicting_rows_in_order() {
    exercise(
        |db| {
            let mut sql = SqlSession::new(db);
            sql.execute("INSERT INTO p VALUES(11,'high-ack')").unwrap();
            assert_eq!(sql.execute("INSERT INTO p VALUES(1,'duplicate'),(12,'new-ack'),(11,'duplicate-high') ON CONFLICT(k) DO NOTHING RETURNING k").unwrap().rows,vec![vec![SqlValue::Int(12)]]);
            assert_eq!(sql.execute("INSERT INTO p VALUES(12,'duplicate-again'),(13,'last-ack') ON CONFLICT DO NOTHING RETURNING k").unwrap().rows,vec![vec![SqlValue::Int(13)]]);
        },
        vec![
            row(1, "original-ack"),
            row(11, "high-ack"),
            row(12, "new-ack"),
            row(13, "last-ack"),
        ],
    );
}

#[test]
fn parent_upsert_uses_parent_policies_even_when_conflict_lives_in_child() {
    exercise(
        |db| {
            let mut sql = SqlSession::new(db);
            sql.execute("CREATE ROLE upsert_writer").unwrap();
            sql.execute("ALTER TABLE p ENABLE ROW LEVEL SECURITY")
                .unwrap();
            sql.execute("CREATE POLICY i ON p FOR INSERT WITH CHECK(k<20)")
                .unwrap();
            sql.execute("CREATE POLICY s ON p FOR SELECT USING(true)")
                .unwrap();
            sql.execute("CREATE POLICY u ON p FOR UPDATE USING(k>=10) WITH CHECK(k<20)")
                .unwrap();
            sql.execute("GRANT SELECT,INSERT,UPDATE ON p TO upsert_writer")
                .unwrap();
            sql.execute("SET ROLE upsert_writer").unwrap();
            assert_eq!(sql.execute("INSERT INTO p VALUES(1,'denied') ON CONFLICT(k) DO UPDATE SET payload=excluded.payload").unwrap_err().sqlstate(),"42501");
            assert!(sql
                .execute("INSERT INTO p VALUES(1,'nothing') ON CONFLICT(k) DO NOTHING RETURNING k")
                .unwrap()
                .rows
                .is_empty());
            assert!(sql.execute("INSERT INTO p VALUES(1,'where-false') ON CONFLICT(k) DO UPDATE SET payload=excluded.payload WHERE false RETURNING k").unwrap().rows.is_empty());
            assert_eq!(
                sql.execute("INSERT INTO low VALUES(2,'direct-child')")
                    .unwrap_err()
                    .sqlstate(),
                "42501"
            );
            sql.execute("RESET ROLE").unwrap();
        },
        vec![row(1, "original-ack")],
    );
}

#[test]
fn upsert_key_change_inside_leaf_is_atomic_but_cross_partition_move_is_rejected() {
    exercise(
        |db| {
            let mut sql = SqlSession::new(db);
            assert_eq!(sql.execute("INSERT INTO p VALUES(1,'must-refuse') ON CONFLICT(k) DO UPDATE SET k=11,payload=excluded.payload").unwrap_err().sqlstate(),"0A000");
            assert_eq!(sql.execute("INSERT INTO p VALUES(1,'moved-within-leaf') ON CONFLICT(k) DO UPDATE SET k=2,payload=excluded.payload RETURNING p.k,p.payload").unwrap().rows,vec![row(2,"moved-within-leaf")]);
            assert_eq!(sql.execute("INSERT INTO p VALUES(2,'first'),(2,'second') ON CONFLICT(k) DO UPDATE SET payload=excluded.payload").unwrap_err().sqlstate(),"21000");
        },
        vec![row(2, "moved-within-leaf")],
    );
}

#[test]
fn later_unroutable_upsert_rolls_back_earlier_leaf_update() {
    exercise(
        |db| {
            let mut sql = SqlSession::new(db);
            assert_eq!(sql.execute("INSERT INTO p VALUES(1,'must-roll-back'),(50,'no-partition') ON CONFLICT(k) DO UPDATE SET payload=excluded.payload").unwrap_err().sqlstate(),"23514");
        },
        vec![row(1, "original-ack")],
    );
}
