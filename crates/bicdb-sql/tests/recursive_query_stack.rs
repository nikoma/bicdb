use bicdb_core::BicDb;
use bicdb_sql::{infer_query_result_columns_for_describe, oracle, SqlSession};

fn correlated_predicate(profile: &'static str) {
    // Match ordinary protocol worker stack size. No larger test stack may mask
    // recursive AST traversal exhausting the worker before rows are executed.
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            let dir = tempfile::tempdir().unwrap();
            let mut db = BicDb::open(dir.path()).unwrap();
            oracle::ensure_database_profile(&mut db, profile).unwrap();
            let lower = |db: &BicDb, sql: &str| {
                if oracle::is_oracle_profile(profile) {
                    oracle::lower_session_sql(db, sql, profile, None, "bicdb").unwrap()
                } else {
                    sql.to_owned()
                }
            };
            for sql in [
                "CREATE TABLE tasks(task_key INT PRIMARY KEY,tenant_key INT,organization_key INT,responsible_key INT,user_key INT,processed TEXT,state TEXT)",
                "CREATE TABLE responsibles(responsible_key INT PRIMARY KEY,kind TEXT,user_key INT,role_key INT)",
                "CREATE TABLE user_roles(user_key INT,role_key INT,enabled TEXT)",
                "CREATE TABLE approvers(task_key INT,user_key INT,enabled TEXT)",
                "CREATE TABLE private_access(record_key INT,table_key INT,user_key INT,enabled TEXT)",
                "INSERT INTO tasks VALUES(1,1,1,1,1,'N','OS'),(2,1,1,1,2,'N','OS'),(3,1,1,2,3,'Y','OS')",
                "INSERT INTO responsibles VALUES(1,'H',1,0),(2,'R',NULL,9)",
            ] {
                let sql = lower(&db, sql);
                SqlSession::new(&mut db).execute(&sql).unwrap();
            }
            let source = include_str!("fixtures/correlated_predicate.sql");
            let sql = lower(&db, source);
            let columns = infer_query_result_columns_for_describe(&db, &sql).unwrap();
            assert_eq!(columns.unwrap().len(), 1);
            let result = SqlSession::new(&mut db).execute(&sql).unwrap();
            assert_eq!(result.rows[0][0].to_cell(), "2");
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn oracle_correlated_predicate_fits_protocol_worker_stack() {
    correlated_predicate(oracle::ORACLE_PREVIEW_PROFILE);
}

#[test]
fn postgres_correlated_predicate_fits_protocol_worker_stack() {
    correlated_predicate("postgres");
}
