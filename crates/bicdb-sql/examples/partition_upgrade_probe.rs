//! Qualify only copies of stores made by an actual older standard binary.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::json;
use std::path::PathBuf;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert_eq!(args.len(), 3, "MODE CASE DATABASE_COPY");
    let mode = StorageMode::from_name(&args[0]);
    let case = &args[1];
    let path = PathBuf::from(&args[2]);
    let config = DbConfig::default()
        .with_storage_mode(mode)
        .with_fsync(true)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    for reopen in 0..3 {
        let mut db = BicDb::open_with_config(&path, config.clone())?;
        match case.as_str() {
            "parent_heap" => {
                let mut sql = SqlSession::new(&mut db);
                if reopen == 0 {
                    let before = sql.execute("SELECT k,payload FROM p ORDER BY k")?;
                    assert_eq!(
                        before.rows.len(),
                        4,
                        "all independently acknowledged old parent/child rows must remain visible"
                    );
                    sql.execute("INSERT INTO p VALUES(1,'updated-parent-ack') ON CONFLICT(k) DO UPDATE SET payload=excluded.payload")?;
                }
                let rows = sql.execute("SELECT k,payload FROM p ORDER BY k")?.rows;
                println!("{}", json!({"case":case,"reopen":reopen,"rows":rows}));
                assert_eq!(rows,vec![vec![SqlValue::Int(1),SqlValue::String("updated-parent-ack".into())],vec![SqlValue::Int(2),SqlValue::String("child-low-ack".into())],vec![SqlValue::Int(11),SqlValue::String("parent-high-ack".into())],vec![SqlValue::Int(12),SqlValue::String("child-high-ack".into())]],"an old parent ACK must arbitrate in its actual destination, never create a second logical primary key");
                assert_eq!(
                    sql.execute("SELECT k FROM p WHERE payload='parent-high-ack'")?
                        .rows,
                    vec![vec![SqlValue::Int(11)]]
                );
                drop(sql);
                assert!(
                    db.scan_collection("p")?.is_empty(),
                    "completed upgrade must evacuate the legacy parent heap"
                );
                assert!(db
                    .verify_indexes_consistently()?
                    .iter()
                    .all(|report| report.valid));
            }
            "bad_child_bound" | "independent_child_policy" => {
                let mut sql = SqlSession::new(&mut db);
                let query = if case == "bad_child_bound" {
                    "SELECT k,payload FROM p ORDER BY k"
                } else {
                    "SET ROLE legacy_reader; SELECT k,payload FROM high ORDER BY k"
                };
                let result = sql.execute(query);
                println!(
                    "{}",
                    json!({"case":case,"reopen":reopen,"observed":format!("{result:?}")})
                );
                let error=result.expect_err("legacy invalid rows or ambiguous policy semantics require an explicit validated upgrade decision before admission");
                assert_eq!(error.sqlstate(), "55000");
                drop(sql);
                if case == "bad_child_bound" {
                    let rows = db.scan_collection("low")?;
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].metadata["payload"], json!("out-of-bound-child-ack"));
                } else {
                    assert_eq!(
                        db.scan_collection("low")?.len() + db.scan_collection("high")?.len(),
                        2
                    );
                    let schema = db.get("__bicdb_pg_schema", "high")?.unwrap();
                    assert!(schema.metadata["policies"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|policy| policy["name"] == "authored_child"));
                }
            }
            "reader_fence" => {
                SqlSession::new(&mut db).execute("SELECT k,payload FROM p ORDER BY k")?;
            }
            _ => panic!("unknown case"),
        }
        db.close()?;
    }
    Ok(())
}
