use bicdb_core::{BicDb, CollectionPolicy, DbConfig, SecurityContext, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
fn seed(sql: &mut SqlSession<'_>) {
    sql.execute("CREATE TABLE inventory(id uuid PRIMARY KEY,n int,tenant text)")
        .unwrap();
    for n in [8, 2, 4, 6, 1, 3, 5, 7] {
        sql.execute(&format!(
            "INSERT INTO inventory VALUES('00000000-0000-0000-0000-{n:012x}',{n},'{}')",
            if n % 2 == 0 { "north" } else { "south" }
        ))
        .unwrap();
    }
}
fn numbers(sql: &mut SqlSession<'_>, query: &str) -> Vec<i64> {
    sql.execute(query)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| match row[0] {
            SqlValue::Int(n) => n,
            _ => panic!("integer result expected"),
        })
        .collect()
}
fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}
#[test]
fn uuid_ordered_pages_preserve_bounds_aliases_direction_and_deleted_keys() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    seed(&mut sql);
    assert_eq!(numbers(&mut sql,"SELECT n FROM inventory WHERE id>'00000000-0000-0000-0000-000000000002'::uuid ORDER BY id LIMIT 2"),vec![3,4]);
    assert_eq!(numbers(&mut sql,"SELECT i.n FROM inventory i WHERE i.id<='00000000-0000-0000-0000-000000000006'::uuid ORDER BY i.id DESC LIMIT 2"),vec![6,5]);
    assert_eq!(
        sql.execute("SELECT id::text AS key FROM inventory ORDER BY id LIMIT 0")
            .unwrap()
            .columns,
        vec!["key"]
    );
    assert!(sql
        .execute("SELECT id FROM inventory WHERE id>NULL ORDER BY id LIMIT 2")
        .unwrap()
        .rows
        .is_empty());
    sql.execute("DELETE FROM inventory WHERE n=1").unwrap();
    assert_eq!(
        numbers(&mut sql, "SELECT n FROM inventory ORDER BY id LIMIT 2"),
        vec![2, 3]
    );
    sql.execute("BEGIN READ ONLY").unwrap();
    assert_eq!(
        numbers(&mut sql, "SELECT n FROM inventory ORDER BY id DESC LIMIT 2"),
        vec![8, 7]
    );
    sql.execute("ROLLBACK").unwrap();
}
#[test]
fn pending_writes_keep_the_callers_visibility_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    seed(&mut sql);
    sql.execute("BEGIN;INSERT INTO inventory VALUES('00000000-0000-0000-0000-000000000000',0,'north');DELETE FROM inventory WHERE n=1").unwrap();
    assert_eq!(
        numbers(&mut sql, "SELECT n FROM inventory ORDER BY id LIMIT 3"),
        vec![0, 2, 3]
    );
    sql.execute("ROLLBACK").unwrap();
    assert_eq!(
        numbers(&mut sql, "SELECT n FROM inventory ORDER BY id LIMIT 3"),
        vec![1, 2, 3]
    );
}
#[test]
fn policies_residual_filters_windows_and_offsets_keep_full_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let mut sql = SqlSession::new(&mut db);
    seed(&mut sql);
    assert_eq!(numbers(&mut sql,"SELECT n FROM inventory WHERE id>'00000000-0000-0000-0000-000000000001'::uuid AND tenant='north' ORDER BY id LIMIT 2"),vec![2,4]);
    assert_eq!(
        numbers(
            &mut sql,
            "SELECT n FROM inventory ORDER BY id LIMIT 2 OFFSET 3"
        ),
        vec![4, 5]
    );
    assert_eq!(
        numbers(
            &mut sql,
            "SELECT row_number() OVER(ORDER BY id) FROM inventory ORDER BY id DESC LIMIT 2"
        ),
        vec![8, 7]
    );
    sql.execute("CREATE ROLE inventory_reader;GRANT SELECT ON inventory TO inventory_reader;ALTER TABLE inventory ENABLE ROW LEVEL SECURITY;ALTER TABLE inventory FORCE ROW LEVEL SECURITY;CREATE POLICY north_only ON inventory FOR SELECT USING(tenant='north');SET ROLE inventory_reader").unwrap();
    assert_eq!(
        numbers(&mut sql, "SELECT n FROM inventory ORDER BY id LIMIT 2"),
        vec![2, 4]
    );
}

#[test]
fn authenticated_administrators_still_obey_core_collection_policies() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    {
        let mut sql = SqlSession::new(&mut db);
        seed(&mut sql);
    }
    db.set_collection_policy(
        "inventory",
        CollectionPolicy::tenant_field("tenant").with_read_roles(["reader"]),
    )
    .unwrap();
    let mut sql = SqlSession::new_secure(
        &mut db,
        SecurityContext::new("inventory-owner", "north").with_roles(["reader"]),
    );
    assert_eq!(
        numbers(&mut sql, "SELECT n FROM inventory ORDER BY id LIMIT 2"),
        vec![2, 4]
    );
}
