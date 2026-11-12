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
    if case.starts_with("entry_") {
        let mut original_hashes = Vec::new();
        for reopen in 0..3 {
            let mut db = BicDb::open_with_config(&path, config.clone())?;
            if reopen == 0 {
                for source in ["p", "low", "high"] {
                    for row in db.scan_collection(source)? {
                        original_hashes.push(row.content_hash()?);
                    }
                }
                let before = db.last_commit_seq();
                let error = bicdb_sql::SqlEngine::new(&db)
                    .execute("SELECT k FROM p")
                    .unwrap_err();
                assert_eq!(
                    error.sqlstate(),
                    "55000",
                    "read-only execution never upgrades a legacy store"
                );
                let error = SqlSession::new_shared(&db)
                    .execute("SELECT k FROM p")
                    .unwrap_err();
                assert!(error
                    .to_string()
                    .contains("operation requires exclusive database access"));
                assert_eq!(db.last_commit_seq(), before);
                assert!(!db.sql_partition_execution_feature_active());
                let mut sql = SqlSession::new(&mut db);
                match case.as_str() {
                    "entry_text" => {
                        assert_eq!(sql.execute("SELECT k FROM p ORDER BY k")?.rows.len(), 4);
                    }
                    "entry_ast" => {
                        let ast = sqlparser::parser::Parser::parse_sql(
                            &sqlparser::dialect::PostgreSqlDialect {},
                            "SELECT k FROM p ORDER BY k",
                        )?
                        .remove(0);
                        assert_eq!(sql.execute_statement_ast(&ast)?.rows.len(), 4);
                    }
                    "entry_copy" => {
                        assert_eq!(
                            sql.copy_insert_rows(
                                "p",
                                &["k".into(), "payload".into()],
                                vec![vec![Some("3".into()), Some("entry-copy-ack".into())]]
                            )?,
                            1
                        );
                    }
                    "entry_procedure" => {
                        sql.call_procedure("add_partition_ack", &[])?;
                    }
                    _ => panic!("unknown entry case"),
                }
            }
            assert!(db.sql_partition_execution_feature_active());
            assert!(db.scan_collection("p")?.is_empty());
            let physical = [db.scan_collection("low")?, db.scan_collection("high")?].concat();
            let hashes = physical
                .iter()
                .map(|row| row.content_hash())
                .collect::<Result<Vec<_>, _>>()?;
            assert!(
                original_hashes.iter().all(|hash| hashes.contains(hash)),
                "every independent old ACK retains its exact full record content"
            );
            let extra = matches!(case.as_str(), "entry_copy" | "entry_procedure");
            assert_eq!(physical.len(), if extra { 5 } else { 4 });
            let expected = if extra {
                vec![1, 2, 3, 11, 12]
            } else {
                vec![1, 2, 11, 12]
            };
            let actual = SqlSession::new(&mut db)
                .execute("SELECT k FROM p ORDER BY k")?
                .rows;
            assert_eq!(
                actual,
                expected
                    .into_iter()
                    .map(|key| vec![SqlValue::Int(key)])
                    .collect::<Vec<_>>()
            );
            assert!(db
                .verify_indexes_consistently()?
                .iter()
                .all(|report| report.valid));
            println!(
                "{}",
                json!({"case":case,"reopen":reopen,"ack_records_preserved":original_hashes.len(),"rows":physical.len()})
            );
            db.close()?;
        }
        return Ok(());
    }
    let mut completed_plan = None;
    for reopen in 0..3 {
        let mut db = BicDb::open_with_config(&path, config.clone())?;
        if let Some(plan) = &completed_plan {
            let sequence = db.last_commit_seq();
            let repeated = bicdb_sql::apply_partition_upgrade(&mut db, Some(plan))?;
            assert!(repeated.already_applied);
            assert_eq!(db.last_commit_seq(), sequence);
        }
        if reopen == 0 && case.starts_with("apply_") {
            let inspection = bicdb_sql::inspect_partition_upgrade(&db)?;
            let mut plan = bicdb_sql::PartitionUpgradePlan {
                snapshot: inspection.snapshot.clone(),
                ..Default::default()
            };
            if case.contains("policy_") {
                let decision = if case.ends_with("keep_stored") {
                    bicdb_sql::PartitionPolicyDecision::KeepStored
                } else {
                    bicdb_sql::PartitionPolicyDecision::PreserveHistoricalEffective
                };
                for conflict in &inspection.policy_decisions_required {
                    plan.policies
                        .insert(conflict.relation.clone(), decision.clone());
                }
            }
            if case == "apply_bad_child_bound" {
                for row in &inspection.invalid_rows {
                    plan.row_relocations
                        .push(bicdb_sql::PartitionRowRelocation {
                            source: row.source.clone(),
                            record_id: row.record_id.clone(),
                            destination: row
                                .suggested_destination
                                .clone()
                                .expect("routable controlled old row"),
                        });
                }
            }
            let applied = bicdb_sql::apply_partition_upgrade(&mut db, Some(&plan))?;
            assert!(!applied.already_applied);
            let sequence = db.last_commit_seq();
            assert!(bicdb_sql::apply_partition_upgrade(&mut db, Some(&plan))?.already_applied);
            assert_eq!(db.last_commit_seq(), sequence);
            completed_plan = Some(plan);
            println!("{}", json!({"applied":applied}));
            assert!(db.sql_partition_execution_feature_active());
            assert!(bicdb_sql::inspect_partition_upgrade(&db)?
                .legacy_relations
                .is_empty());
        }
        match case.as_str() {
            "inspect" => {
                let report = bicdb_sql::inspect_partition_upgrade(&db)?;
                println!("{}", serde_json::to_string(&report)?);
            }
            "parent_heap" | "apply_parent_heap" => {
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
            "apply_bad_child_bound" => {
                assert!(db.scan_collection("low")?.is_empty());
                let raw = db.scan_collection("high")?;
                assert_eq!(raw.len(), 1);
                assert_eq!(raw[0].metadata["payload"], json!("out-of-bound-child-ack"));
                assert_eq!(
                    SqlSession::new(&mut db)
                        .execute("SELECT k,payload FROM p")?
                        .rows,
                    vec![vec![
                        SqlValue::Int(11),
                        SqlValue::String("out-of-bound-child-ack".into())
                    ]]
                );
            }
            "apply_policy_keep_stored" | "apply_policy_preserve_historical" => {
                assert_eq!(
                    db.scan_collection("low")?.len() + db.scan_collection("high")?.len(),
                    2
                );
                let archive = db.scan_collection("__bicdb_pg_partition_policy_archive")?;
                assert_eq!(archive.len(), 2);
                assert!(archive
                    .iter()
                    .any(
                        |record| record.metadata["original_record"]["metadata"]["policies"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|policy| policy["name"] == "authored_child")
                    ));
                let mut sql = SqlSession::new(&mut db);
                let low = sql
                    .execute("SET ROLE legacy_reader;SELECT k FROM low ORDER BY k")?
                    .rows;
                let high = sql.execute("SELECT k FROM high ORDER BY k")?.rows;
                if case.ends_with("keep_stored") {
                    assert!(low.is_empty());
                    assert_eq!(high, vec![vec![SqlValue::Int(11)]]);
                } else {
                    assert_eq!(low, vec![vec![SqlValue::Int(2)]]);
                    assert!(high.is_empty());
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
