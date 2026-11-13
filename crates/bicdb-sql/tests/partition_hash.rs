//! Expected physical leaves were collected from real PostgreSQL 16.13 INSERTs,
//! not computed with the implementation being tested.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{SqlSession, SqlValue};
use serde_json::Value;

fn literal(value: &Value) -> String {
    match value.as_str() {
        Some(text) => format!("'{}'", text.replace('\'', "''")),
        None => "NULL".into(),
    }
}

#[test]
fn actual_pg16_hash_partitions_match_physical_rows_and_repeated_reopens() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/postgres/partition_hash_pg16.json")).unwrap();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
        let mut db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
        let cases = [
            ("integer", "hi", "k BIGINT,label TEXT", "k"),
            ("text", "ht", "k TEXT,label TEXT", "k"),
            ("float", "hf", "k DOUBLE PRECISION,label TEXT", "k"),
            ("mixed", "hm", "k BIGINT,t TEXT,label TEXT", "k,t"),
        ];
        {
            let mut sql = SqlSession::new(&mut db);
            for (kind, table, columns, key) in cases {
                sql.execute(&format!(
                    "CREATE TABLE {table}({columns}) PARTITION BY HASH({key})"
                ))
                .unwrap();
                for r in 0..16 {
                    sql.execute(&format!("CREATE TABLE {table}_{r} PARTITION OF {table} FOR VALUES WITH(MODULUS 16,REMAINDER {r})")).unwrap();
                }
                for case in fixture[kind].as_array().unwrap() {
                    let values = if kind == "mixed" {
                        format!(
                            "{},{},{}",
                            literal(&case["key"]),
                            literal(&case["text"]),
                            literal(&case["label"])
                        )
                    } else {
                        format!("{},{}", literal(&case["key"]), literal(&case["label"]))
                    };
                    sql.execute(&format!("INSERT INTO {table} VALUES({values})"))
                        .unwrap();
                    let wrong = (case["remainder"].as_u64().unwrap() + 1) % 16;
                    assert_eq!(
                        sql.execute(&format!("INSERT INTO {table}_{wrong} VALUES({values})"))
                            .unwrap_err()
                            .sqlstate(),
                        "23514",
                        "{kind} {case}"
                    );
                }
            }
            sql.execute("CREATE TABLE variable_modulus(k BIGINT,label TEXT) PARTITION BY HASH(k)")
                .unwrap();
            for (name, modulus, remainder) in [("even", 2, 0), ("odd1", 4, 1), ("odd3", 4, 3)] {
                sql.execute(&format!("CREATE TABLE {name} PARTITION OF variable_modulus FOR VALUES WITH(MODULUS {modulus},REMAINDER {remainder})")).unwrap();
            }
            for case in fixture["integer"].as_array().unwrap() {
                sql.execute(&format!(
                    "INSERT INTO variable_modulus VALUES({},{})",
                    literal(&case["key"]),
                    literal(&case["label"])
                ))
                .unwrap();
            }
        }
        for reopen in 0..3 {
            let mut sql = SqlSession::new(&mut db);
            for (kind, table, _, _) in cases {
                for r in 0..16 {
                    let expected = fixture[kind]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|case| case["remainder"].as_u64() == Some(r))
                        .map(|case| vec![SqlValue::String(case["label"].as_str().unwrap().into())])
                        .collect::<Vec<_>>();
                    assert_eq!(
                        sql.execute(&format!("SELECT label FROM {table}_{r} ORDER BY label"))
                            .unwrap()
                            .rows,
                        expected,
                        "{kind} leaf{r} reopen{reopen}"
                    );
                }
                assert_eq!(
                    sql.execute(&format!("SELECT count(*) FROM {table}"))
                        .unwrap()
                        .rows,
                    vec![vec![SqlValue::Int(
                        fixture[kind].as_array().unwrap().len() as i64
                    )]]
                );
            }
            for name in ["even", "odd1", "odd3"] {
                let expected = fixture["integer"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|case| {
                        let r = case["remainder"].as_u64().unwrap();
                        match name {
                            "even" => r % 2 == 0,
                            "odd1" => r % 4 == 1,
                            _ => r % 4 == 3,
                        }
                    })
                    .map(|case| vec![SqlValue::String(case["label"].as_str().unwrap().into())])
                    .collect::<Vec<_>>();
                assert_eq!(
                    sql.execute(&format!("SELECT label FROM {name} ORDER BY label"))
                        .unwrap()
                        .rows,
                    expected
                );
            }
            drop(sql);
            if reopen < 2 {
                db.close().unwrap();
                db = BicDb::open_with_config(dir.path(), cfg.clone()).unwrap();
            }
        }
        db.close().unwrap();
    }
}
