use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};
#[test]
fn audit_boundaries() {
    let f: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/typed_comparison/boundary_audit.json"
    ))
    .unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        bicdb_core::DbConfig::default().with_fsync(false),
    )
    .unwrap();
    let mut s = SqlSession::new(&mut db);
    for st in f["setup"].as_array().unwrap() {
        let sql = st.as_str().unwrap();
        s.execute(sql)
            .unwrap_or_else(|e| panic!("setup {sql}: {e}"));
    }
    for case in f["cases"].as_array().unwrap() {
        let mut result = serde_json::json!({"label":case["label"],"sql":case["sql"]});
        match s.execute(case["sql"].as_str().unwrap()) {
            Ok(r) => {
                result["rows"] = serde_json::json!(r
                    .rows
                    .iter()
                    .map(|row| row
                        .iter()
                        .map(|v| if matches!(v, SqlValue::Null) {
                            "<null>".to_string()
                        } else {
                            v.to_cell()
                        })
                        .collect::<Vec<_>>())
                    .collect::<Vec<_>>())
            }
            Err(e) => {
                result["sqlstate"] = serde_json::json!(e.sqlstate());
                result["error"] = serde_json::json!(e.to_string());
            }
        }
        eprintln!("AUDIT_RESULT {result}");
    }
}
