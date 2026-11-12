//! Startup-only old-store qualification: never binds a listener.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_pgwire::{PgWireConfig, PgWireError, PgWireServer};
use bicdb_sql::{SqlEngine, SqlValue};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert_eq!(args.len(), 3, "MODE CASE DATABASE_COPY");
    let mode = StorageMode::from_name(&args[0]);
    let case = &args[1];
    let path = PathBuf::from(&args[2]);
    for reopen in 0..3 {
        let opened = PgWireServer::open(
            &path,
            PgWireConfig {
                fsync: true,
                automatic_paged_checkpoint: false,
                ..Default::default()
            },
        );
        if case == "parent_heap" {
            let server = opened?;
            drop(server);
            let db = BicDb::open_with_config(
                &path,
                DbConfig::default()
                    .with_storage_mode(mode.clone())
                    .with_fsync(true)
                    .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
            )?;
            assert!(db.sql_partition_execution_feature_active());
            assert!(db.scan_collection("p")?.is_empty());
            let rows = SqlEngine::new(&db)
                .execute("SELECT k FROM p ORDER BY k")?
                .rows;
            assert_eq!(
                rows,
                [1, 2, 11, 12]
                    .into_iter()
                    .map(|key| vec![SqlValue::Int(key)])
                    .collect::<Vec<_>>()
            );
            assert!(db
                .verify_indexes_consistently()?
                .iter()
                .all(|report| report.valid));
            db.close()?;
        } else {
            match opened {
                Err(PgWireError::Sql(error)) => assert_eq!(error.sqlstate(), "55000"),
                Err(error) => return Err(error.into()),
                Ok(_) => panic!(
                    "ambiguous/invalid old stores must refuse before a server can be returned"
                ),
            }
            let db = BicDb::open_with_config(
                &path,
                DbConfig::default()
                    .with_storage_mode(mode.clone())
                    .with_fsync(true)
                    .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
            )?;
            assert!(!db.sql_partition_execution_feature_active());
            if case == "bad_child_bound" {
                assert_eq!(db.scan_collection("low")?.len(), 1);
            } else {
                assert_eq!(
                    db.scan_collection("low")?.len() + db.scan_collection("high")?.len(),
                    2
                );
                assert!(
                    db.get("__bicdb_pg_schema", "high")?.unwrap().metadata["policies"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|policy| policy["name"] == "authored_child")
                );
            }
            db.close()?;
        }
        println!(
            "{}",
            serde_json::json!({"case":case,"reopen":reopen,"startup_only":true})
        );
    }
    Ok(())
}
