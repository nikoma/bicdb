//! Native values, result OIDs and failures for element-polymorphic routines.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{pg_type_oid_by_name, SqlSession, SqlValue};

#[test]
fn polymorphic_execution_and_result_types_follow_actual_arguments() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/postgres/element_polymorphic_pg16.json"
    ))
    .unwrap();
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let open = || {
            BicDb::open_with_config(
                root.path(),
                DbConfig::default().with_storage_mode(mode.clone()),
            )
            .unwrap()
        };
        {
            let mut db = open();
            let mut session = SqlSession::new(&mut db);
            for sql in fixture["setup"].as_array().unwrap() {
                session.execute(sql.as_str().unwrap()).unwrap();
            }
            check_calls(&mut session, &fixture);
        }
        let mut db = open();
        let mut session = SqlSession::new(&mut db);
        // Alternate concrete types again with cold per-database caches.
        check_calls(&mut session, &fixture);
        check_calls(&mut session, &fixture);
        assert_eq!(session.execute("SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname='ident' AND pronamespace='poly_probe'::regnamespace").unwrap().rows, vec![vec![SqlValue::String("anyelement".into())]]);
    }
}

fn check_calls(session: &mut SqlSession<'_>, fixture: &serde_json::Value) {
    for check in fixture["checks"].as_array().unwrap() {
        let sql = check["query"].as_str().unwrap();
        if let Some(sqlstate) = check["sqlstate"].as_str() {
            let error = session.execute(sql).unwrap_err();
            assert_eq!(error.sqlstate(), sqlstate, "{sql}: {error}");
            continue;
        }
        let result = session
            .execute(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        let actual = result
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| match cell {
                        SqlValue::Null => None,
                        cell => Some(cell.to_cell()),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let expected = check["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                row.as_array()
                    .unwrap()
                    .iter()
                    .map(|cell| match cell {
                        serde_json::Value::Null => None,
                        serde_json::Value::String(value) => Some(value.clone()),
                        value => Some(value.to_string()),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{sql}");
        let actual_oids = result
            .column_types
            .iter()
            .map(|kind| kind.as_deref().and_then(pg_type_oid_by_name))
            .collect::<Vec<_>>();
        let expected_oids = check["oids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|oid| Some(oid.as_i64().unwrap() as i32))
            .collect::<Vec<_>>();
        assert_eq!(
            actual_oids, expected_oids,
            "{sql}: {:?}",
            result.column_types
        );
    }
}
