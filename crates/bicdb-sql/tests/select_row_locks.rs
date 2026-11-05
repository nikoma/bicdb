use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn row_locks_skip_contended_rows_before_limit_and_follow_savepoints() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE jobs (id int PRIMARY KEY, tenant text, rank int)")
            .unwrap();
        sql.execute("INSERT INTO jobs VALUES (1,'a',1),(2,'a',2),(3,'a',3)")
            .unwrap();
        sql.execute("CREATE TABLE installations (tenant text PRIMARY KEY)")
            .unwrap();
        sql.execute("INSERT INTO installations VALUES ('a')")
            .unwrap();
    }
    let mut first = SqlSession::new_shared(&db);
    let mut second = SqlSession::new_shared(&db);
    first.execute("BEGIN").unwrap();
    first
        .execute("SELECT id FROM jobs WHERE id=1 FOR UPDATE")
        .unwrap();
    second.execute("BEGIN").unwrap();
    let result = second.execute("SELECT COALESCE(string_agg(candidate.id::text, ','),'') FROM (SELECT j.id, j.rank FROM jobs j JOIN installations i ON i.tenant=j.tenant ORDER BY j.rank,j.id LIMIT 1 FOR UPDATE OF j SKIP LOCKED) candidate").unwrap();
    assert_eq!(result.rows, vec![vec![SqlValue::String("2".into())]]);
    second.execute("ROLLBACK").unwrap();
    second.execute("BEGIN").unwrap();
    let error = second
        .execute("SELECT id FROM jobs WHERE id=1 FOR UPDATE NOWAIT")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "55P03");
    second.execute("ROLLBACK").unwrap();
    first.execute("SAVEPOINT added_lock").unwrap();
    first
        .execute("SELECT id FROM jobs WHERE id=2 FOR UPDATE")
        .unwrap();
    first.execute("ROLLBACK TO added_lock").unwrap();
    second.execute("BEGIN").unwrap();
    assert_eq!(
        second
            .execute("SELECT id FROM jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );
    second.execute("ROLLBACK").unwrap();
    first.execute("ROLLBACK").unwrap();
    second.execute("BEGIN").unwrap();
    assert_eq!(
        second
            .execute("SELECT id FROM jobs ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    second.execute("ROLLBACK").unwrap();
}

#[test]
fn row_locks_contend_with_mutations_and_enforce_privileges() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE ledger (id int PRIMARY KEY, amount int)")
            .unwrap();
        sql.execute("INSERT INTO ledger VALUES (1,10),(2,20)")
            .unwrap();
        sql.execute("CREATE ROLE reader").unwrap();
        sql.execute("GRANT SELECT ON ledger TO reader").unwrap();
    }
    let mut reader = SqlSession::new_shared_unprivileged(&db, "reader");
    assert!(reader.execute("SELECT id FROM ledger FOR UPDATE").is_err());
    let mut holder = SqlSession::new_shared(&db);
    holder.execute("BEGIN").unwrap();
    holder
        .execute("SELECT id FROM ledger WHERE id=1 FOR UPDATE")
        .unwrap();
    std::thread::scope(|scope| {
        let (done, result) = std::sync::mpsc::channel();
        let db = &db;
        scope.spawn(move || {
            let mut writer = SqlSession::new_shared(db);
            writer.execute("BEGIN").unwrap();
            let update = writer.execute("UPDATE ledger SET amount=11 WHERE id=1");
            writer.execute("ROLLBACK").unwrap();
            done.send(update.is_ok()).unwrap();
        });
        let early = result.recv_timeout(std::time::Duration::from_millis(100));
        // Release before asserting so a failed expectation cannot strand the
        // scoped writer behind a lock still held by the test thread.
        holder.execute("ROLLBACK").unwrap();
        assert!(early.is_err(), "writer completed while the row was locked");
        assert!(result
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap());
    });
    let mut writer = SqlSession::new_shared(&db);
    writer.execute("BEGIN").unwrap();
    writer
        .execute("UPDATE ledger SET amount=11 WHERE id=1")
        .unwrap();
    holder.execute("BEGIN").unwrap();
    assert_eq!(
        holder
            .execute("SELECT id FROM ledger ORDER BY id FOR UPDATE SKIP LOCKED")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );
    holder.execute("ROLLBACK").unwrap();
    writer.execute("ROLLBACK").unwrap();
}

#[test]
fn row_locks_do_not_lock_another_roles_rls_hidden_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE TABLE inbox (id int PRIMARY KEY, tenant text)")
            .unwrap();
        sql.execute("INSERT INTO inbox VALUES (1,'alice'),(2,'bob')")
            .unwrap();
        sql.execute("CREATE ROLE alice").unwrap();
        sql.execute("CREATE ROLE bob").unwrap();
        sql.execute("GRANT SELECT, UPDATE ON inbox TO alice, bob")
            .unwrap();
        sql.execute("ALTER TABLE inbox ENABLE ROW LEVEL SECURITY")
            .unwrap();
        sql.execute("CREATE POLICY read_visibility ON inbox FOR SELECT USING (true)")
            .unwrap();
        sql.execute(
            "CREATE POLICY lock_visibility ON inbox FOR UPDATE USING (tenant = current_user)",
        )
        .unwrap();
    }
    let mut alice = SqlSession::new_shared_unprivileged(&db, "alice");
    let mut bob = SqlSession::new_shared_unprivileged(&db, "bob");
    alice.execute("BEGIN").unwrap();
    assert_eq!(
        alice
            .execute("SELECT id FROM inbox FOR UPDATE")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1)]]
    );
    bob.execute("BEGIN").unwrap();
    assert_eq!(
        bob.execute("SELECT id FROM inbox FOR UPDATE NOWAIT")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );
    bob.execute("ROLLBACK").unwrap();
    alice.execute("ROLLBACK").unwrap();
}

#[test]
fn blocking_row_wait_honors_cancellation_and_releases_transaction_locks_on_rollback() {
    use bicdb_core::CancellationToken;
    use std::sync::{atomic::AtomicBool, Arc};
    use std::time::{Duration, Instant};
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut setup = SqlSession::new(&mut db);
    setup
        .execute("CREATE TABLE cancellation_rows(id INT PRIMARY KEY)")
        .unwrap();
    setup
        .execute("INSERT INTO cancellation_rows VALUES (1),(2)")
        .unwrap();
    drop(setup);
    let mut owner = SqlSession::new_shared(&db);
    owner.execute("BEGIN").unwrap();
    owner
        .execute("SELECT id FROM cancellation_rows WHERE id=1 FOR UPDATE")
        .unwrap();
    let mut waiter = SqlSession::new_shared(&db);
    waiter.execute("BEGIN").unwrap();
    waiter
        .execute("SELECT id FROM cancellation_rows WHERE id=2 FOR UPDATE")
        .unwrap();
    waiter = waiter.with_cancellation(CancellationToken::new(
        Arc::new(AtomicBool::new(false)),
        Some(Instant::now() + Duration::from_millis(100)),
    ));
    let error = waiter
        .execute("SELECT id FROM cancellation_rows WHERE id=1 FOR UPDATE")
        .unwrap_err();
    assert_eq!(error.sqlstate(), "57014");
    waiter = waiter.with_cancellation(CancellationToken::uncancelable());
    waiter.execute("ROLLBACK").unwrap();
    assert_eq!(
        owner
            .execute("SELECT id FROM cancellation_rows WHERE id=2 FOR UPDATE NOWAIT")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(2)]]
    );
    owner.execute("ROLLBACK").unwrap();
}

#[test]
fn lateral_table_functions_preserve_base_row_locks_and_skip_locked_limits() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut setup = SqlSession::new(&mut db);
    setup.execute("CREATE TABLE lateral_jobs(id int PRIMARY KEY,owner int,nodes jsonb); CREATE TABLE lateral_owners(id int PRIMARY KEY); INSERT INTO lateral_owners VALUES(1); INSERT INTO lateral_jobs VALUES(1,1,'[1,2]'),(2,1,'[3,4]')").unwrap();
    drop(setup);
    let mut holder = SqlSession::new_shared(&db);
    let mut worker = SqlSession::new_shared(&db);
    let mut observer = SqlSession::new_shared(&db);
    holder.execute("BEGIN").unwrap();
    holder
        .execute("SELECT id FROM lateral_jobs WHERE id=1 FOR UPDATE")
        .unwrap();
    worker.execute("BEGIN").unwrap();
    let result = worker.execute("SELECT j.id,node FROM lateral_jobs j JOIN lateral_owners o ON o.id=j.owner CROSS JOIN LATERAL jsonb_array_elements(j.nodes) node ORDER BY j.id,node LIMIT 1 FOR UPDATE SKIP LOCKED").unwrap();
    assert_eq!(
        result.rows,
        vec![vec![
            SqlValue::Int(2),
            SqlValue::Json(serde_json::json!(3).into())
        ]]
    );
    observer.execute("BEGIN").unwrap();
    assert_eq!(
        observer
            .execute("SELECT id FROM lateral_jobs WHERE id=2 FOR UPDATE NOWAIT")
            .unwrap_err()
            .sqlstate(),
        "55P03"
    );
    observer.execute("ROLLBACK; BEGIN").unwrap();
    assert_eq!(
        observer
            .execute("SELECT id FROM lateral_owners WHERE id=1 FOR UPDATE NOWAIT")
            .unwrap_err()
            .sqlstate(),
        "55P03"
    );
    observer.execute("ROLLBACK").unwrap();
    worker.execute("ROLLBACK").unwrap();
    holder.execute("ROLLBACK").unwrap();
    observer.execute("BEGIN").unwrap();
    assert_eq!(
        observer
            .execute("SELECT id FROM lateral_jobs ORDER BY id FOR UPDATE NOWAIT")
            .unwrap()
            .rows
            .len(),
        2
    );
    observer.execute("ROLLBACK").unwrap();
}

#[test]
fn explicit_lateral_function_lock_targets_are_refused_without_weakening_base_privileges() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(dir.path()).unwrap();
    let mut setup = SqlSession::new(&mut db);
    setup.execute("CREATE TABLE lateral_private_jobs(id int PRIMARY KEY,nodes jsonb); INSERT INTO lateral_private_jobs VALUES(1,'[1]'); CREATE ROLE lateral_reader; GRANT SELECT ON lateral_private_jobs TO lateral_reader").unwrap();
    assert_eq!(setup.execute("SELECT j.id FROM lateral_private_jobs j CROSS JOIN LATERAL jsonb_array_elements(j.nodes) node FOR UPDATE OF node").unwrap_err().sqlstate(),"0A000");
    drop(setup);
    let mut reader = SqlSession::new_shared_unprivileged(&db, "lateral_reader");
    assert_eq!(reader.execute("SELECT j.id FROM lateral_private_jobs j CROSS JOIN LATERAL jsonb_array_elements(j.nodes) node FOR UPDATE").unwrap_err().sqlstate(),"42501");
}
