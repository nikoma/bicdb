use bicdb_core::BicDb;
use bicdb_sql::{SqlSession, SqlValue};

#[test]
fn cold_monitoring_helpers_compile_on_an_ordinary_worker_stack() {
    let directory = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(directory.path()).unwrap();
        SqlSession::new(&mut db)
            .execute(include_str!("fixtures/ehr-monitoring-validator.sql"))
            .unwrap();
    }
    let config: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/ehr-monitoring-config.json")).unwrap();
    for valid in [true, false] {
        let path = directory.path().to_path_buf();
        let mut config = config.clone();
        if !valid {
            config["forms"][0]["mappings"][0]["measurement"] =
                serde_json::Value::String("unknown_measurement".into());
        }
        // Routine IR caches are thread-local. Creating declarations on another
        // thread must not warm the helpers compiled inside nested evaluation.
        std::thread::Builder::new()
            .name("cold-monitoring-compiler".into())
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let mut db = BicDb::open(path).unwrap();
                let mut session = SqlSession::new(&mut db);
                let text = config.to_string().replace('\'', "''");
                let result = session
                    .execute(&format!(
                        "SELECT monitoring_validate_config('{text}'::jsonb)"
                    ))
                    .unwrap();
                let SqlValue::Json(result) = &result.rows[0][0] else {
                    panic!("JSON validator result required")
                };
                assert_eq!(result["valid"], serde_json::Value::Bool(valid));
                if !valid {
                    assert!(result["errors"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|error| { error.as_str().unwrap().contains("unknown measurement") }));
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
