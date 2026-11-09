use bicdb_core::BicDb;
use bicdb_sql::SqlSession;

const FIXTURE: &str = include_str!("fixtures/typed_comparison/typed_comparison_pg16.json");

#[test]
fn debug_queries() {
    let fixture: serde_json::Value = serde_json::from_str(FIXTURE).unwrap();
    for indexed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(root.path(), bicdb_core::DbConfig::default().with_fsync(false)).unwrap();
        let mut s = SqlSession::new(&mut db);
        s.execute("SET TimeZone = 'UTC'").unwrap();
        for st in fixture["setup"].as_array().unwrap() { s.execute(st.as_str().unwrap()).unwrap(); }
        if indexed { for c in ["id", "i8", "d", "ts"] { s.execute(&format!("CREATE INDEX m_{c} ON m ({c})")).unwrap(); } }
        s.execute("CREATE TABLE kk (k int4, v int4)").unwrap();
        s.execute("INSERT INTO kk SELECT g, g * 10 FROM generate_series(1, 12) g").unwrap();
        s.execute("CREATE TABLE ki (id int4, v int4)").unwrap();
        s.execute("INSERT INTO ki SELECT g, g * 10 FROM generate_series(1, 12) g").unwrap();
        if indexed { s.execute("CREATE INDEX kk_k ON kk (k)").unwrap(); s.execute("CREATE INDEX ki_id ON ki (id)").unwrap(); }
        for q in [
            "SELECT vc AS x FROM m UNION SELECT c FROM m",
            "SELECT tx AS x FROM m UNION SELECT c FROM m",
            "SELECT count(*) FROM kk x JOIN kk y ON x.k = y.k",
            "SELECT count(*) FROM ki x JOIN ki y ON x.id = y.id",
            "SELECT count(*) FROM ki x JOIN ki y ON x.v = y.v",
            "SELECT x.id, y.id, y.i8, x.i2 FROM m x JOIN m y ON x.id = y.id AND x.i2 < y.i8 ORDER BY 1, 2",
            "SELECT x.id, y.id, y.i8 FROM m x JOIN m y ON x.id = y.id ORDER BY 1, 2",
            "SELECT x.id, y.id FROM m x JOIN m y ON x.id = y.id AND y.i8 > 0 ORDER BY 1, 2",
            "SELECT x.id, y.id FROM m x JOIN m y ON x.id = y.id AND x.i8 > 0 ORDER BY 1, 2",
            "SELECT x.id, y.id FROM m x JOIN m y ON x.id = y.id AND x.i2 < y.i8 ORDER BY 1, 2",
            "SELECT x.id, y.id FROM m x JOIN m y ON x.id = y.id AND x.i4 < y.i4 + 1 ORDER BY 1, 2",
            "SELECT x.id, y.id FROM m x JOIN m y ON x.id = y.id WHERE x.i2 < y.i8 ORDER BY 1, 2",
            "SELECT id FROM m o WHERE EXISTS (SELECT 1 FROM m i WHERE i.d = o.ts AND i.id = o.id) ORDER BY id",
            "SELECT id FROM m o WHERE EXISTS (SELECT 1 FROM m i WHERE i.id = o.id AND i.d = o.ts) ORDER BY id",
            "SELECT id FROM m WHERE d IN (SELECT ts FROM m) ORDER BY id",
            "SELECT count(*) FROM (SELECT vc AS x FROM m UNION SELECT c FROM m) u",
            "SELECT x FROM (SELECT vc AS x FROM m UNION SELECT c FROM m) u ORDER BY 1",
            "SELECT rows_d_ts()", "SELECT x.id, y.id FROM m x JOIN m y ON x.d = y.ts ORDER BY 1, 2", "SELECT id FROM m WHERE d = ts ORDER BY id",
        ] {
            let explain = s.execute(&format!("EXPLAIN {q}")).map(|r| r.rows.iter().map(|row| row.iter().map(|v| v.to_cell()).collect::<Vec<_>>().join(" ")).collect::<Vec<_>>().join("\n"));
            let rows = s.execute(q).map(|r| r.rows.iter().map(|row| row.iter().map(|v| v.to_cell()).collect::<Vec<_>>().join(",")).collect::<Vec<_>>().join(" | "));
            let types = s.execute(q).map(|r| format!("{:?}", r.column_types));
            eprintln!("### indexed={indexed} {q}\n{explain:?}\n=> {rows:?}\ntypes {types:?}\n");
        }
    }
}
