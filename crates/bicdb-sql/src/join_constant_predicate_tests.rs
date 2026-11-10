//! Selective constant expressions must bound intermediates before a join.
use crate::*;
use bicdb_core::{DbConfig, StorageMode};

fn fixture(mode: StorageMode, count: usize) -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_storage_mode(mode)).unwrap();
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE TABLE constant_left(id int, join_key int, name text); CREATE TABLE constant_right(id int, join_key int)").unwrap();
    let left = (0..count)
        .map(|i| format!("({i},1,'{}')", if i == 0 { "target" } else { "other" }))
        .collect::<Vec<_>>()
        .join(",");
    let right = (0..count)
        .map(|i| format!("({i},1)"))
        .collect::<Vec<_>>()
        .join(",");
    sql.execute(&format!(
        "INSERT INTO constant_left VALUES {left}; INSERT INTO constant_right VALUES {right}"
    ))
    .unwrap();
    (root, db)
}

fn measured<T>(operation: impl FnOnce() -> T) -> (T, SqlProfileStats) {
    SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().push(SqlProfileStats::default()));
    let value = operation();
    let stats = SQL_PROFILE_STACK.with(|stack| stack.borrow_mut().pop().unwrap());
    (value, stats)
}

#[test]
fn literal_scalar_operands_filter_before_materializing_a_join() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let (_root, mut db) = fixture(mode, 128);
        let (result, stats) = measured(|| {
            SqlSession::new(&mut db).execute("SELECT c.id,d.id FROM constant_left c,constant_right d WHERE c.join_key=d.join_key AND c.name=lower('TARGET') ORDER BY d.id").unwrap()
        });
        assert_eq!(
            result.rows,
            (0..128)
                .map(|i| vec![SqlValue::Int(0), SqlValue::Int(i)])
                .collect::<Vec<_>>()
        );
        assert!(
            stats.join_intermediate_rows <= 1024,
            "a selective scalar constant must not leave a quadratic intermediate: {stats:?}"
        );
    }
}

#[test]
fn routine_operands_refresh_for_every_call_before_join_planning() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let (_root, mut db) = fixture(mode, 128);
        let mut sql = SqlSession::new(&mut db);
        sql.execute("CREATE FUNCTION constant_join_count(search_name text) RETURNS int LANGUAGE plpgsql AS $$ DECLARE n int; BEGIN SELECT count(*) INTO n FROM constant_left c,constant_right d WHERE c.join_key=d.join_key AND c.name=lower(search_name); RETURN n; END $$").unwrap();
        for (name, expected) in [("TARGET", 128), ("ABSENT", 0), ("TARGET", 128)] {
            let (result, stats) = measured(|| {
                sql.execute(&format!("SELECT constant_join_count('{name}')"))
                    .unwrap()
            });
            assert_eq!(result.rows, vec![vec![SqlValue::Int(expected)]]);
            assert!(
                stats.join_intermediate_rows <= 1024,
                "routine constants must not create a quadratic intermediate: {stats:?}"
            );
        }
    }
}

#[test]
fn user_overloads_and_column_arguments_keep_their_resolution() {
    let (_root, mut db) = fixture(StorageMode::EmbeddedMemory, 4);
    let mut sql = SqlSession::new(&mut db);
    sql.execute("CREATE FUNCTION public.lower(value text) RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'other'::text $$; SET search_path=public,pg_catalog").unwrap();
    assert_eq!(sql.execute("SELECT count(*) FROM constant_left c,constant_right d WHERE c.join_key=d.join_key AND c.name=lower('TARGET'::text)").unwrap().rows, vec![vec![SqlValue::Int(12)]]);
    sql.execute("DROP FUNCTION public.lower(text)").unwrap();
    assert_eq!(sql.execute("SELECT count(*) FROM constant_left c,constant_right d WHERE c.join_key=d.join_key AND lower(c.name)='target'").unwrap().rows, vec![vec![SqlValue::Int(4)]]);
}

#[test]
fn volatile_calls_are_not_evaluated_before_an_empty_join() {
    let (_root, mut db) = fixture(StorageMode::EmbeddedMemory, 4);
    let mut sql = SqlSession::new(&mut db);
    sql.execute("TRUNCATE constant_right; CREATE SEQUENCE constant_eval_seq; CREATE FUNCTION public.lower(value text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$ BEGIN PERFORM nextval('constant_eval_seq'); RETURN 'other'; END $$; SET search_path=public,pg_catalog").unwrap();
    assert_eq!(sql.execute("SELECT count(*) FROM constant_left c,constant_right d WHERE c.join_key=d.join_key AND c.name=lower('TARGET'::text) AND nextval('constant_eval_seq')>0").unwrap().rows, vec![vec![SqlValue::Int(0)]]);
    assert_eq!(
        sql.execute("SELECT last_value,is_called FROM constant_eval_seq")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Int(1), SqlValue::Bool(false)]]
    );
}
