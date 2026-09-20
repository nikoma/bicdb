use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

fn seed(db: &mut BicDb) {
    SqlSession::new(db).execute("CREATE TABLE left_rows (id INT PRIMARY KEY); CREATE TABLE right_rows (id INT PRIMARY KEY); INSERT INTO left_rows VALUES (1),(2); INSERT INTO right_rows VALUES (1),(2)").unwrap();
}

#[test]
fn skipped_join_releases_new_locks_but_retains_returned_row_locks() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    seed(&mut db);
    let mut holder = SqlSession::new_shared(&db);
    holder.execute("BEGIN").unwrap();
    holder
        .execute("SELECT id FROM right_rows WHERE id=1 FOR UPDATE")
        .unwrap();
    let mut worker = SqlSession::new_shared(&db);
    worker.execute("BEGIN").unwrap();
    assert_eq!(worker.execute("SELECT l.id FROM left_rows l JOIN right_rows r ON r.id=l.id ORDER BY l.id FOR UPDATE SKIP LOCKED").unwrap().rows, vec![vec![SqlValue::Int(2)]]);
    let mut probe = SqlSession::new_shared(&db);
    assert_eq!(
        probe
            .execute("SELECT id FROM left_rows WHERE id=1 FOR UPDATE NOWAIT")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    assert_eq!(
        probe
            .execute("SELECT id FROM left_rows WHERE id=2 FOR UPDATE NOWAIT")
            .unwrap_err()
            .sqlstate(),
        "55P03"
    );
    worker.execute("ROLLBACK").unwrap();
    holder.execute("ROLLBACK").unwrap();
}

#[test]
fn skip_preserves_locks_held_before_the_candidate_or_statement() {
    for prelock in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(dir.path()).unwrap();
        seed(&mut db);
        let mut holder = SqlSession::new_shared(&db);
        holder.execute("BEGIN").unwrap();
        holder
            .execute("SELECT id FROM right_rows WHERE id=2 FOR UPDATE")
            .unwrap();
        let mut worker = SqlSession::new_shared(&db);
        worker.execute("BEGIN").unwrap();
        if prelock {
            worker
                .execute("SELECT id FROM left_rows WHERE id=1 FOR UPDATE")
                .unwrap();
        }
        let selection = if prelock {
            "l.id=1 AND r.id=2"
        } else {
            "l.id=1"
        };
        let rows = worker.execute(&format!("SELECT r.id FROM left_rows l CROSS JOIN right_rows r WHERE {selection} ORDER BY r.id FOR UPDATE SKIP LOCKED")).unwrap().rows;
        assert_eq!(
            rows,
            if prelock {
                vec![]
            } else {
                vec![vec![SqlValue::Int(1)]]
            }
        );
        let mut probe = SqlSession::new_shared(&db);
        assert_eq!(
            probe
                .execute("SELECT id FROM left_rows WHERE id=1 FOR UPDATE NOWAIT")
                .unwrap_err()
                .sqlstate(),
            "55P03"
        );
        worker.execute("ROLLBACK").unwrap();
        assert_eq!(
            probe
                .execute("SELECT id FROM left_rows WHERE id=1 FOR UPDATE NOWAIT")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(1)]]
        );
        holder.execute("ROLLBACK").unwrap();
    }
}
