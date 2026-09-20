use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn comma_and_cross_joins_find_rows_without_a_bound_right_table_predicate() {
    for paged in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let config = if paged {
            DbConfig::default().with_storage_mode(StorageMode::ServerPaged)
        } else {
            DbConfig::default()
        };
        let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE stock (s_i_id INT NOT NULL, s_w_id INT NOT NULL, s_price NUMERIC(5,2), PRIMARY KEY (s_w_id, s_i_id)); INSERT INTO stock VALUES (2,1,2.5); CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_tax NUMERIC(4,4), w_name VARCHAR(10)); INSERT INTO warehouse VALUES (1,0.0750,'first')").unwrap();
        let control = session.execute("SELECT s.s_i_id, w.w_tax FROM stock s JOIN warehouse w ON s.s_w_id=w.w_id WHERE s.s_i_id=2").unwrap().rows;
        assert_eq!(control.len(), 1);
        assert_eq!(control[0][0], SqlValue::Int(2));
        for sql in [
            "SELECT s_i_id, w_tax FROM stock, warehouse WHERE s_w_id=w_id AND s_i_id=2",
            "SELECT s_i_id, w_tax FROM stock, warehouse WHERE w_id=1 AND s_w_id=w_id AND s_i_id=2",
            "SELECT s.s_i_id, w.w_tax FROM stock s CROSS JOIN warehouse w WHERE s.s_w_id=w.w_id AND s.s_i_id=2",
            "SELECT s.s_i_id, w.w_tax FROM warehouse w, stock s WHERE s.s_w_id=w.w_id AND s.s_i_id=2",
        ] {
            assert_eq!(session.execute(sql).unwrap().rows, control, "paged={paged}: {sql}");
        }
        session.execute("INSERT INTO warehouse VALUES (2,0.1000,'second'); INSERT INTO stock VALUES (2,2,3.0),(2,3,4.0)").unwrap();
        for from in ["stock s, warehouse w", "stock s CROSS JOIN warehouse w"] {
            assert_eq!(session.execute(&format!("SELECT s.s_w_id FROM {from} WHERE s.s_w_id=w.w_id AND s.s_i_id=2 ORDER BY s.s_w_id")).unwrap().rows, vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]], "paged={paged}: {from}");
            assert!(session
                .execute(&format!(
                    "SELECT s.s_i_id FROM {from} WHERE s.s_w_id=w.w_id AND s.s_i_id=99"
                ))
                .unwrap()
                .rows
                .is_empty());
        }
    }
}

#[test]
fn unjoined_values_are_not_null_or_index_bounds() {
    for paged in [false, true] {
        for index in ["", "CREATE INDEX values_key ON items (join_key)"] {
            let dir = tempfile::tempdir().unwrap();
            let config = if paged {
                DbConfig::default().with_storage_mode(StorageMode::ServerPaged)
            } else {
                DbConfig::default()
            };
            let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
            let mut session = SqlSession::new(&mut db);
            session.execute("CREATE TABLE items (id INT PRIMARY KEY, join_key INT); CREATE TABLE lookup (key_value INT); INSERT INTO items VALUES (1,10),(2,20),(3,NULL); INSERT INTO lookup VALUES (10),(20),(NULL)").unwrap();
            if !index.is_empty() {
                session.execute(index).unwrap();
            }
            for predicate in [
                "a.join_key = b.key_value",
                "b.key_value = a.join_key",
                "a.join_key = b.key_value + 0",
                "b.key_value + 0 = a.join_key",
            ] {
                let sql =
                    format!("SELECT a.id FROM items a, lookup b WHERE {predicate} ORDER BY a.id");
                assert_eq!(
                    session.execute(&sql).unwrap().rows,
                    vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]],
                    "paged={paged}, index={index}: {sql}"
                );
            }
            assert!(session.execute("SELECT a.id FROM items a, lookup b WHERE a.join_key = b.key_value AND a.join_key = NULL").unwrap().rows.is_empty());
            assert_eq!(session.execute("SELECT a.id FROM items a, lookup b WHERE a.join_key = b.key_value AND a.join_key = 20").unwrap().rows, vec![vec![SqlValue::Int(2)]]);
        }
    }
}

#[test]
fn range_index_filters_defer_unjoined_values_with_pending_writes() {
    for paged in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let config = if paged {
            DbConfig::default().with_storage_mode(StorageMode::ServerPaged)
        } else {
            DbConfig::default()
        };
        let mut db = BicDb::open_with_config(dir.path(), config).unwrap();
        let mut session = SqlSession::new(&mut db);
        session.execute("CREATE TABLE entries (entry_id INT, join_key INT); CREATE INDEX entries_range ON entries (entry_id, join_key); CREATE TABLE keys (key_value INT); INSERT INTO entries VALUES (1,10),(2,20),(3,NULL),(4,99); INSERT INTO keys VALUES (10),(20),(NULL)").unwrap();
        for pending in [false, true] {
            if pending {
                session
                    .execute("BEGIN; UPDATE entries SET join_key = 30 WHERE entry_id = 4")
                    .unwrap();
            }
            assert_eq!(session.execute("SELECT a.entry_id FROM entries a, keys b WHERE a.entry_id >= 1 AND a.join_key = b.key_value ORDER BY a.entry_id").unwrap().rows, vec![vec![SqlValue::Int(1)], vec![SqlValue::Int(2)]], "paged={paged}, pending={pending}");
            if pending {
                session.execute("ROLLBACK").unwrap();
            }
        }
    }
}
