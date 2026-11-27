//! `BicDb::index_prefix_entries`: the decoded keys and row ids an index-only
//! scan reads, one entry per committed row, NULL keys included, in key order.

use bicdb_core::{BicDb, IndexValue};
use bicdb_sql::SqlSession;

#[test]
fn entries_follow_committed_rows_in_key_order() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open(root.path()).unwrap();
    SqlSession::new(&mut db)
        .execute(
            "CREATE TABLE e (id int4 PRIMARY KEY, a int4, b int4);
             INSERT INTO e VALUES (1, 2, 20), (2, 1, 10), (3, 2, NULL), (4, NULL, 5), (5, 2, 10);
             CREATE INDEX e_ab ON e (a, b);
             UPDATE e SET a = 3 WHERE id = 5;
             DELETE FROM e WHERE id = 2;",
        )
        .unwrap();
    let all = db.index_prefix_entries("e_ab", &[], 100).unwrap().unwrap();
    assert_eq!(all.len(), 4, "one entry per live row: {all:?}");
    let keys = all.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
    assert!(
        keys.iter().any(|key| key[0] == IndexValue::Null),
        "NULL keys are indexed: {keys:?}"
    );
    let twos = db
        .index_prefix_entries("e_ab", &[IndexValue::Int(2)], 100)
        .unwrap()
        .unwrap();
    assert_eq!(twos.len(), 2, "{twos:?}");
    assert!(twos
        .iter()
        .all(|(key, _)| key[0] == IndexValue::Int(2) && key.len() == 2));
    let threes = db
        .index_prefix_entries("e_ab", &[IndexValue::Int(3)], 100)
        .unwrap()
        .unwrap();
    assert_eq!(threes.len(), 1);
    assert_eq!(threes[0].0, vec![IndexValue::Int(3), IndexValue::Int(10)]);
    // Past the limit the caller is told to read rows instead.
    assert!(db.index_prefix_entries("e_ab", &[], 3).unwrap().is_none());
}
