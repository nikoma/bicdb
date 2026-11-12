//! Crash only disposable copies of genuine old stores. Expected snapshots and
//! the ACK file live outside the database. This is not an application or VM test.
use bicdb_core::{BicDb, DbConfig, StorageMode};
use bicdb_sql::{PartitionPolicyDecision, PartitionRowRelocation, PartitionUpgradePlan};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Snapshot {
    schemas: BTreeMap<String, Value>,
    rows: Vec<(String, String, String, Value)>,
}

fn snapshot(db: &BicDb) -> Result<Snapshot, Box<dyn std::error::Error>> {
    let mut schemas = BTreeMap::new();
    let mut rows = Vec::new();
    for name in ["p", "low", "high"] {
        schemas.insert(
            name.into(),
            db.get("__bicdb_pg_schema", name)?.unwrap().metadata.clone(),
        );
        for row in db.scan_collection(name)? {
            rows.push((
                name.into(),
                row.id.clone(),
                row.content_hash()?,
                serde_json::to_value(row)?,
            ));
        }
    }
    rows.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    Ok(Snapshot { schemas, rows })
}

fn write_new(path: &Path, value: &impl Serialize) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(
        args.len() >= 4,
        "MODE snapshot|apply|verify DATABASE FILE [FILE]"
    );
    let config = DbConfig::default()
        .with_storage_mode(StorageMode::from_name(&args[0]))
        .with_fsync(true)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let path = Path::new(&args[2]);
    match args[1].as_str() {
        "snapshot" => {
            assert_eq!(args.len(), 4);
            let db = BicDb::open_with_config(path, config)?;
            write_new(Path::new(&args[3]), &snapshot(&db)?)?;
            db.close()?;
        }
        "apply" | "io_fail" => {
            assert_eq!(args.len(), 5, "MODE apply DATABASE CASE EXTERNAL_ACK_FILE");
            let mut db = BicDb::open_with_config(path, config)?;
            let before_sequence = db.last_commit_seq();
            let failure = if args[1] == "io_fail" {
                Some(bicdb_page::test_faults::arm_io_failure(
                    &std::env::var("PARTITION_FAIL_SITE")?,
                    db.data_path(),
                    std::env::var("PARTITION_FAIL_NTH")?.parse()?,
                    std::env::var("PARTITION_FAIL_PERSISTENT")? == "1",
                ))
            } else {
                None
            };
            // Arm only after open, scoped to this disposable database. This
            // reaches existing Native I/O boundaries in either storage mode
            // without adding a product-only upgrade shortcut or crash hook.
            let _io_cut = std::env::var("PARTITION_CRASH_IO").ok().map(|spec| {
                let (site, count) = spec.rsplit_once(':').expect("SITE:HIT");
                let site = site.to_string();
                let label = site.clone();
                bicdb_page::test_faults::arm_io_hook(
                    &site,
                    db.data_path(),
                    count.parse().unwrap(),
                    move || {
                        eprintln!("partition crash I/O hook reached: {label}");
                        bicdb_page::test_faults::crash_now(
                            bicdb_page::test_faults::volatile_tail_enabled(),
                        );
                    },
                )
            });
            let inspection = bicdb_sql::inspect_partition_upgrade(&db)?;
            let mut plan = PartitionUpgradePlan {
                snapshot: inspection.snapshot,
                ..Default::default()
            };
            if args[3].starts_with("policy_") {
                let decision = if args[3] == "policy_keep_stored" {
                    PartitionPolicyDecision::KeepStored
                } else {
                    assert_eq!(args[3], "policy_preserve_historical");
                    PartitionPolicyDecision::PreserveHistoricalEffective
                };
                for conflict in inspection.policy_decisions_required {
                    plan.policies.insert(conflict.relation, decision.clone());
                }
            } else if args[3] == "bad_child_bound" {
                for row in inspection.invalid_rows {
                    plan.row_relocations.push(PartitionRowRelocation {
                        source: row.source,
                        record_id: row.record_id,
                        destination: row
                            .suggested_destination
                            .expect("controlled routable old ACK"),
                    });
                }
            } else {
                assert_eq!(args[3], "parent_heap");
            }
            let outcome = bicdb_sql::apply_partition_upgrade(&mut db, Some(&plan));
            if let Some(failure) = failure {
                assert!(failure.fired() > 0, "requested I/O failure was not reached");
                let error = outcome.expect_err("injected I/O failure was acknowledged");
                // Even a one-shot fault must fence this handle after the I/O
                // condition is removed. A failure report is never an ACK.
                drop(failure);
                let later = db.insert(
                    "low",
                    bicdb_core::Record::new("9")
                        .with_metadata(json!({"k":9,"payload":"forbidden-post-failure-write"})),
                );
                assert!(
                    later.is_err(),
                    "failed migration handle acknowledged another write"
                );
                write_new(
                    Path::new(&args[4]),
                    &json!({"upgrade_acknowledged":false,"error":error.to_string(),"sqlstate":error.sqlstate(),"after_failure_write_refused":true,"after_failure_error":later.unwrap_err().to_string()}),
                )?;
                // Preserve the actual failed state for another process. A
                // graceful database close is not this fault's recovery path.
                std::process::exit(0);
            }
            let applied = outcome?;
            assert!(!applied.already_applied);
            // The controller observes this independent, fsynced ACK only after
            // apply returned. Recovery must preserve the complete new state.
            write_new(
                Path::new(&args[4]),
                &json!({"upgrade_acknowledged":true,"applied":applied,
                    "before_native_sequence":before_sequence,"after_native_sequence":db.last_commit_seq()}),
            )?;
            bicdb_page::test_faults::crash_point("partition_upgrade.after_ack");
            db.close()?;
        }
        "verify" => {
            assert_eq!(
                args.len(),
                5,
                "MODE verify DATABASE OLD_SNAPSHOT NEW_SNAPSHOT"
            );
            let old: Snapshot = serde_json::from_reader(File::open(&args[3])?)?;
            let new: Snapshot = serde_json::from_reader(File::open(&args[4])?)?;
            let ack = std::env::var_os("PARTITION_CRASH_ACK").map(std::path::PathBuf::from);
            for reopen in 0..3 {
                let db = BicDb::open_with_config(path, config.clone())?;
                let actual = snapshot(&db)?;
                let complete = actual == new;
                assert!(
                    complete || actual == old,
                    "mixed or altered ACK/schema state: {actual:?}"
                );
                if ack.as_ref().is_some_and(|p| p.exists()) {
                    assert!(complete, "acknowledged upgrade rolled back");
                }
                let certificate = if db.has_collection("__bicdb_pg_partition_certificate") {
                    db.get("__bicdb_pg_partition_certificate", "current")?
                        .is_some()
                } else {
                    false
                };
                if complete {
                    assert!(db.sql_partition_execution_feature_active());
                    assert!(certificate);
                    bicdb_sql::check_sql_partition_admission(&db)?;
                } else {
                    assert!(!certificate, "legacy state has a completion certificate");
                }
                assert!(db
                    .verify_indexes_consistently()?
                    .iter()
                    .all(|report| report.valid));
                println!(
                    "{}",
                    json!({"reopen":reopen,"state":if complete {"complete_new"} else {"complete_old"},"reader_fence":db.sql_partition_execution_feature_active(),"rows":actual.rows.len(),"full_records_and_schemas_equal":true,"native_indexes_valid":true})
                );
                db.close()?;
            }
        }
        _ => panic!("unknown probe action"),
    }
    Ok(())
}
