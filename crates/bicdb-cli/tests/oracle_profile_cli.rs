//! The SQL CLI must retain the durable Oracle profile across analytics dispatch.
use bicdb_core::{BicDb, Record};
use bicdb_sql::SqlSession;
use serde_json::{json, Value};
use std::{collections::BTreeMap, process::Command};
#[test]
fn oracle_grouped_case_keeps_frozen_database_character_set_semantics() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open(root.path()).unwrap();
        SqlSession::new(&mut db).execute("CREATE TABLE case_routes(id INT PRIMARY KEY,device_id TEXT); INSERT INTO case_routes VALUES(1,'ß'),(2,'SS')").unwrap();
        db.create_collection("__bicdb_sql_compatibility").unwrap();
        db.insert(
            "__bicdb_sql_compatibility",
            Record::new("profile").with_metadata(
                json!({"format":1,"profile":bicdb_sql::oracle::ORACLE_PREVIEW_PROFILE}),
            ),
        )
        .unwrap();
        db.flush().unwrap();
    }
    let output=Command::new(env!("CARGO_BIN_EXE_bicdb")).args(["sql",root.path().to_str().unwrap(),"SELECT upper(device_id) AS case_key,count(*) AS amount FROM case_routes GROUP BY upper(device_id) ORDER BY case_key","--json"]).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    let rows = result.get("rows").unwrap_or(&result).as_array().unwrap();
    let actual = rows
        .iter()
        .map(|row| {
            if let Some(row) = row.as_array() {
                (
                    row[0].as_str().unwrap().to_string(),
                    row[1].as_i64().unwrap(),
                )
            } else {
                (
                    row["case_key"].as_str().unwrap().to_string(),
                    row["amount"].as_i64().unwrap(),
                )
            }
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        actual,
        BTreeMap::from([("SS".into(), 1), ("ß".into(), 1)]),
        "{result}"
    );
}
