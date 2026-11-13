//! Source-pinned recovery qualification on disposable, acknowledged stores.
//! The controller owns process/VM cuts; this probe never resets a host or VM.
use bicdb_core::{
    BicDb, BicDbError, DbConfig, MutationActor, MutationGrantSpec, MutationOperation,
    MutationPolicy, Record, StorageMode,
};
use bicdb_sql::{SqlSession, SqlValue};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const AUDIT: &str = "__bicdb_mutation_audit";
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Snapshot {
    schema: Record,
    policy: MutationPolicy,
    rows: Vec<Record>,
}
fn snapshot(db: &BicDb) -> Result<Snapshot> {
    let mut rows = db.scan_collection("computed")?;
    rows.sort_by(|a, b| a.id.cmp(&b.id));
    assert!(
        db.scan_collection("probe_aux")?.is_empty(),
        "failed handle acknowledged another write"
    );
    Ok(Snapshot {
        schema: (*db.get("__bicdb_pg_schema", "computed")?.unwrap()).clone(),
        policy: db.mutation_policy("computed")?.unwrap(),
        rows,
    })
}
fn actor() -> MutationActor {
    MutationActor {
        actor_id: "identity-recovery-host".into(),
        roles: BTreeSet::from(["writer".into()]),
        scopes: BTreeSet::new(),
        tenant_id: None,
        workspace_id: None,
        originating_plugin: "source-pinned-recovery-probe".into(),
        originating_resource: Some("computed".into()),
        originating_action: Some("update".into()),
        trace_id: "one-authorized-identity-update".into(),
        // Fixed disposable host context makes independent audit expectations
        // deterministic. This is never a user/application timeout override.
        deadline_unix_ms: 4_102_444_800_000,
    }
}
fn grant() -> MutationGrantSpec {
    MutationGrantSpec {
        relation: "computed".into(),
        operation: MutationOperation::Update,
        record_id: None,
        record_id_prefix: None,
        expected_version: None,
        version_field: None,
        allowed_columns: ["id", "k", "b", "note"]
            .into_iter()
            .map(String::from)
            .collect(),
        bulk: false,
        maximum_affected_rows: 1,
        cascade_relations: BTreeSet::new(),
        statement_budget: 1,
        tenant_field: None,
        workspace_field: None,
        audit_metadata: BTreeMap::from([("proof".into(), "one actual authorized UPDATE".into())]),
    }
}
fn write_new(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    File::open(path.parent().unwrap())?.sync_all()?;
    Ok(())
}
fn witness(label: &str, value: Value) {
    println!("{label} {value}");
    std::io::stdout()
        .flush()
        .expect("flush independent controller witness");
}
fn pause(site: &str) -> ! {
    witness("IDENTITY_VM_CUT_READY", json!({"site":site}));
    loop {
        std::thread::park();
    }
}
fn audits(db: &BicDb) -> Result<Vec<Value>> {
    Ok(db.with_broker(|broker|broker.peek(AUDIT,0,10))?.into_iter().map(|m|json!({
        "sequence":m.sequence,"message_id":m.message_id,"payload":m.payload,"headers":m.headers,
        "idempotency_key":m.idempotency_key,"created_at":m.created_at,"available_at":m.available_at,"max_attempts":m.max_attempts,
    })).collect())
}
fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(
        args.len() >= 4,
        "MODE seed|apply|io_fail|verify DATABASE EXTERNAL_FILE"
    );
    let cfg = DbConfig::default()
        .with_storage_mode(StorageMode::from_name(&args[0]))
        .with_fsync(true)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let path = Path::new(&args[2]);
    match args[1].as_str() {
        "seed" => {
            assert_eq!(args.len(), 4);
            let mut db = BicDb::open_with_config(path, cfg)?;
            SqlSession::new(&mut db).execute("CREATE TABLE computed(k INT GENERATED ALWAYS AS(b*2) STORED PRIMARY KEY,b INT,note TEXT); CREATE INDEX computed_note_idx ON computed(note); CREATE TABLE probe_aux(id INT PRIMARY KEY); INSERT INTO computed(b,note) VALUES(1,'original-ACK'),(5,'untouched-ACK')")?;
            let mut row = (*db.get("computed", "2")?.unwrap()).clone();
            row.metadata["native_ack"] = json!("outside-SQL-schema-must-survive");
            row.vector = Some(vec![0.25, 0.5, 0.75]);
            row.timestamp = Some(1_700_000_001);
            row.payload = Some(vec![0, 1, 2, 255]);
            db.insert("computed", row)?;
            db.set_mutation_policy("computed", MutationPolicy::grants_required().with_audit())?;
            assert!(audits(&db)?.is_empty());
            let state = snapshot(&db)?;
            assert_eq!(state.rows.len(), 2);
            write_new(Path::new(&args[3]), &state)?;
            witness(
                "IDENTITY_SEED_ACK",
                json!({"acknowledged_rows":state.rows.iter().map(|r|json!({"id":r.id,"hash":r.content_hash().unwrap()})).collect::<Vec<_>>()}),
            );
            db.close()?;
        }
        "apply" | "io_fail" => {
            assert_eq!(args.len(), 4);
            let mut db = BicDb::open_with_config(path, cfg)?;
            let before_sequence = db.last_commit_seq();
            let tx = db.begin_application_transaction(actor())?;
            let txid = tx.id().0;
            witness(
                "IDENTITY_TX_STARTED",
                json!({"native_tx_id":txid,"old_id":"2","new_id":"4"}),
            );
            let failure = if args[1] == "io_fail" {
                Some(bicdb_page::test_faults::arm_io_failure(
                    &std::env::var("IDENTITY_FAIL_SITE")?,
                    db.data_path(),
                    std::env::var("IDENTITY_FAIL_NTH")?.parse()?,
                    std::env::var("IDENTITY_FAIL_PERSISTENT")? == "1",
                ))
            } else {
                None
            };
            let _io_cut = std::env::var("IDENTITY_CRASH_IO").ok().map(|spec| {
                let (site, nth) = spec.rsplit_once(':').expect("SITE:HIT");
                let label = site.to_owned();
                bicdb_page::test_faults::arm_io_hook(
                    site,
                    db.data_path(),
                    nth.parse().unwrap(),
                    move || {
                        eprintln!("identity crash I/O hook reached: {label}");
                        bicdb_page::test_faults::crash_now(
                            bicdb_page::test_faults::volatile_tail_enabled(),
                        );
                    },
                )
            });
            let _vm_pause = std::env::var("IDENTITY_VM_PAUSE_IO").ok().map(|spec| {
                let (site, nth) = spec.rsplit_once(':').expect("SITE:HIT");
                let label = site.to_owned();
                bicdb_page::test_faults::arm_io_hook(
                    site,
                    db.data_path(),
                    nth.parse().unwrap(),
                    move || pause(&label),
                )
            });
            let outcome = (|| -> Result<()> {
                let mut tx = tx;
                let authority = tx.issue_mutation_grant(grant())?;
                let mut sql = SqlSession::new_shared(&db)
                    .with_pending_transaction(tx)
                    .with_signed_mutation_grants(BTreeMap::from([("computed".into(), authority)]));
                assert_eq!(
                    sql.execute("UPDATE computed SET b=2 WHERE k=2 RETURNING k,b")?
                        .rows,
                    vec![vec![SqlValue::Int(4), SqlValue::Int(2)]]
                );
                let tx = sql.take_pending_transaction().unwrap();
                drop(sql);
                assert_eq!(tx.write_len(), 2);
                assert_eq!(tx.broker_publish_len(), 1);
                bicdb_page::test_faults::crash_point("identity.after_staging");
                tx.commit()?;
                if std::env::var("IDENTITY_VM_PAUSE_AFTER_COMMIT").as_deref() == Ok("1") {
                    pause("identity.after_commit");
                }
                bicdb_page::test_faults::crash_point("identity.after_commit");
                Ok(())
            })();
            if let Some(fault) = failure {
                let fired = fault.fired();
                assert!(fired > 0, "requested real I/O fault did not fire");
                let error = outcome.expect_err("I/O failure was acknowledged");
                drop(fault);
                let later = db.insert("probe_aux", Record::new("forbidden"));
                assert!(
                    matches!(later, Err(BicDbError::WriteFailed { .. })),
                    "failed handle accepted write or only refused authorization: {later:?}"
                );
                write_new(
                    Path::new(&args[3]),
                    &json!({"update_acknowledged":false,"native_tx_id":txid,"fault_fired":fired,"error":error.to_string(),"after_failure_write_refused":true,"after_failure_error":later.unwrap_err().to_string()}),
                )?;
                std::process::exit(0);
            }
            outcome?;
            let observed = audits(&db)?;
            assert_eq!(observed.len(), 1);
            let ack = json!({"update_acknowledged":true,"native_tx_id":txid,"before_native_sequence":before_sequence,"after_native_sequence":db.last_commit_seq(),"audit_message_id":observed[0]["message_id"],"old_id":"2","new_id":"4"});
            write_new(Path::new(&args[3]), &ack)?;
            if std::env::var("IDENTITY_VM_PAUSE_AFTER_ACK").as_deref() == Ok("1") {
                witness("IDENTITY_VM_ACK", ack);
                pause("identity.after_ack");
            }
            bicdb_page::test_faults::crash_point("identity.after_ack");
            db.close()?;
        }
        "verify" => {
            assert_eq!(args.len(), 4, "MODE verify DATABASE ORIGINAL_SEED_ACK");
            let old: Snapshot = serde_json::from_reader(File::open(&args[3])?)?;
            let mut new = old.clone();
            let row = new.rows.iter_mut().find(|r| r.id == "2").unwrap();
            row.id = "4".into();
            row.metadata["k"] = json!(4);
            row.metadata["b"] = json!(2);
            new.rows.sort_by(|a, b| a.id.cmp(&b.id));
            let ack = std::env::var_os("IDENTITY_ACK").map(std::path::PathBuf::from);
            let require_new = std::env::var("IDENTITY_REQUIRE_NEW").as_deref() == Ok("1")
                || ack.as_ref().is_some_and(|p| p.is_file());
            let expected_tx = std::env::var("IDENTITY_EXPECTED_TX_ID")?.parse::<u64>()?;
            let mut stable = None;
            for reopen in 0..3 {
                let mut db = BicDb::open_with_config(path, cfg.clone())?;
                let actual = snapshot(&db)?;
                let is_new = actual == new;
                assert!(is_new || actual==old,"mixed/altered acknowledged row, schema, Native attributes or mutation policy: {actual:?}");
                if require_new {
                    assert!(
                        is_new,
                        "externally acknowledged identity update rolled back"
                    );
                }
                let audit = audits(&db)?;
                if is_new {
                    assert_eq!(
                        audit.len(),
                        1,
                        "recovered identity update must have exactly one audit"
                    );
                    let payload = &audit[0]["payload"];
                    assert_eq!(
                        payload,
                        &json!({"transaction_id":expected_tx,"grant_id":1,"relation":"computed","operation":"update","record_count":1,"record_ids":["2"],"record_ids_truncated":false,"actor":actor(),"audit_metadata":grant().audit_metadata,"replacement_record_ids":["4"]})
                    );
                    assert_eq!(
                        audit[0]["idempotency_key"],
                        format!("mutation-audit/{expected_tx}/1/1")
                    );
                    assert_eq!(audit[0]["headers"]["actor_id"], actor().actor_id);
                    if let Some(p) = ack.as_ref().filter(|p| p.is_file()) {
                        let a: Value = serde_json::from_reader(File::open(p)?)?;
                        assert_eq!(a["native_tx_id"], expected_tx);
                        assert_eq!(a["audit_message_id"], audit[0]["message_id"]);
                    }
                } else {
                    assert!(audit.is_empty(), "aborted update published an audit");
                }
                let current = (actual.clone(), audit.clone());
                if let Some(previous) = &stable {
                    assert_eq!(
                        &current, previous,
                        "reopen changed rows or duplicated/altered the audit"
                    );
                }
                stable = Some(current);
                assert!(db.verify_indexes_consistently()?.iter().all(|r| r.valid));
                let sequence = db.last_commit_seq();
                assert!(matches!(
                    db.insert("computed", Record::new("999")),
                    Err(BicDbError::MutationDenied(_))
                ));
                assert_eq!(db.last_commit_seq(), sequence);
                witness(
                    "IDENTITY_VERIFIED",
                    json!({"reopen":reopen,"state":if is_new {"complete_new"}else{"complete_old"},"full_records_schema_policy_equal":true,"audit_count":audit.len(),"audit_messages":audit,"native_indexes_valid":true,"grantless_write_refused":true,"row_hashes":actual.rows.iter().map(|r|json!({"id":r.id,"hash":r.content_hash().unwrap()})).collect::<Vec<_>>()}),
                );
                db.close()?;
            }
        }
        _ => panic!("unknown action"),
    }
    Ok(())
}
