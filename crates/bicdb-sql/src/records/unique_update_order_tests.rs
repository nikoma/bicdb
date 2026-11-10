//! Fix the visited-row order explicitly: SQL plans need not use heap order.
use super::*;

#[test]
fn immediate_unique_checks_keep_future_old_keys_but_release_preceding_old_keys() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default()
                .with_fsync(false)
                .with_storage_mode(mode),
        )
        .unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE ordered_keys(id TEXT PRIMARY KEY,owner INT,seq INT,UNIQUE(owner,seq));INSERT INTO ordered_keys VALUES('r3',7,3),('r4',7,4)").unwrap();
        let schema = load_schema(&db, "ordered_keys").unwrap().unwrap();
        for (order, swap, expected) in [
            ([3, 4], false, "00000"),
            ([4, 3], false, "23505"),
            ([3, 4], true, "23505"),
        ] {
            let pairs = order
                .into_iter()
                .map(|seq| {
                    let before = db
                        .get("ordered_keys", &format!("r{seq}"))
                        .unwrap()
                        .unwrap()
                        .as_ref()
                        .clone();
                    let mut after = before.clone();
                    after.metadata["seq"] = serde_json::json!(if swap { 7 - seq } else { seq - 1 });
                    (before, after)
                })
                .collect::<Vec<_>>();
            let result =
                validate_unique_constraints_for_update(&db, None, "ordered_keys", &schema, &pairs);
            assert_eq!(
                result.as_ref().err().map_or("00000", |e| e.sqlstate()),
                expected,
                "order={order:?} swap={swap}: {result:?}"
            );
        }
        assert_eq!(
            SqlSession::new(&mut db)
                .execute("SELECT seq FROM ordered_keys ORDER BY seq")
                .unwrap()
                .rows,
            vec![vec![SqlValue::Int(3)], vec![SqlValue::Int(4)]]
        );
    }
}
