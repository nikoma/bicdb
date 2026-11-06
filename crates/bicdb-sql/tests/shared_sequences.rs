use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
use std::sync::{Arc, Barrier};

#[test]
fn shared_sequence_allocations_are_unique_durable_and_survive_outer_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE SEQUENCE shared_counter")
        .unwrap();
    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(8));
    let handles = (0..8)
        .map(|_| {
            let db = db.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut sql = SqlSession::new_shared(&db);
                let mut values = Vec::new();
                barrier.wait();
                for _ in 0..25 {
                    sql.execute("BEGIN").unwrap();
                    let r = sql.execute("SELECT nextval('shared_counter')").unwrap();
                    let SqlValue::Int(value) = r.rows[0][0] else {
                        panic!("int8 sequence value required")
                    };
                    values.push(value);
                    sql.execute("ROLLBACK").unwrap();
                }
                values
            })
        })
        .collect::<Vec<_>>();
    let mut values = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(values, (1..=200).collect::<Vec<_>>());
    drop(db);
    let mut db = BicDb::open(dir.path()).unwrap();
    let r = SqlSession::new(&mut db)
        .execute("SELECT nextval('shared_counter')")
        .unwrap();
    assert_eq!(r.rows, vec![vec![SqlValue::Int(201)]]);
}

#[test]
fn shared_batch_defaults_and_setval_keep_nontransactional_sequence_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    SqlSession::new(&mut db)
        .execute("CREATE TABLE shared_rows(id bigserial PRIMARY KEY,label text)")
        .unwrap();
    let mut sql = SqlSession::new_shared(&db);
    sql.execute("BEGIN").unwrap();
    sql.execute("INSERT INTO shared_rows(label) VALUES('a'),('b'),('c')")
        .unwrap();
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute("SELECT count(*) FROM shared_rows")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(0)]]
    );
    assert_eq!(
        sql.execute("SELECT nextval('shared_rows_id_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(4)]]
    );
    sql.execute("BEGIN").unwrap();
    sql.execute("SELECT setval('shared_rows_id_seq',40,false)")
        .unwrap();
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        sql.execute("SELECT nextval('shared_rows_id_seq')")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(40)]]
    );
}
