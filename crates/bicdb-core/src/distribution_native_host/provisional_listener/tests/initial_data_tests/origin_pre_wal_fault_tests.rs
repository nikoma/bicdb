//! Genuine INITIAL NativeHost/TLS/public-client fault isolation. This required
//! recovery gate is expected RED until an actual automatic controller exists.
//! No synthetic certificate, phase reset, new genesis or manual repair is used.
use super::*;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;

struct Journal {
    file: std::sync::Mutex<File>,
    context: std::sync::Mutex<Option<serde_json::Value>>,
    sequence: AtomicU64,
    bytes: AtomicU64,
}
impl Journal {
    fn create(path: &Path) -> io::Result<Self> {
        let parent = path.parent().unwrap();
        std::fs::create_dir_all(parent)?;
        let file = OpenOptions::new().create_new(true).write(true).open(path)?;
        file.sync_all()?;
        // Diagnostic ancestors only, never an original dbN directory.
        for ancestor in std::fs::canonicalize(parent)?.ancestors() {
            File::open(ancestor)?.sync_all()?;
        }
        Ok(Self {
            file: std::sync::Mutex::new(file),
            context: std::sync::Mutex::new(None),
            sequence: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        })
    }
    fn append(&self, mut value: serde_json::Value) -> io::Result<()> {
        value["journal_sequence"] = (self.sequence.fetch_add(1, Ordering::AcqRel) + 1).into();
        value["unix_ns"] = serde_json::json!(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos());
        let mut encoded = serde_json::to_vec(&value)?;
        encoded.push(b'\n');
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("journal poisoned"))?;
        let previous = self.bytes.fetch_add(encoded.len() as u64, Ordering::AcqRel);
        if previous.saturating_add(encoded.len() as u64) > 64 * 1024 * 1024 {
            return Err(io::Error::other(
                "complete diagnostic journal exceeds64MiB; no truncation",
            ));
        }
        file.write_all(&encoded)?;
        file.sync_all()
    }
    fn start(
        &self,
        operation: &str,
        key: &str,
        value: Option<&serde_json::Value>,
    ) -> io::Result<Uuid> {
        let id = Uuid::now_v7();
        let context = serde_json::json!({"diagnostic_business_id":id,
            "diagnostic_id_is_wire_business_id":false,"operation":operation,
            "event_id":crate::cluster_event_record_id("items",key).unwrap(),"key":key,
            "payload":value,"payload_sha256":value.map(|v|hex::encode(Sha256::digest(serde_json::to_vec(v).unwrap()))),
            "request_timeout_ms":2000,"wire_command_hash_not_supplied_by_client":true});
        self.append(serde_json::json!({"kind":"logical_intent","context":context}))?;
        *self
            .context
            .lock()
            .map_err(|_| io::Error::other("context poisoned"))? = Some(context);
        Ok(id)
    }
    fn finish(&self, id: Uuid, outcome: serde_json::Value, receipt_ns: u128) -> io::Result<()> {
        self.append(
            serde_json::json!({"kind":"logical_public_reply","diagnostic_business_id":id,
            "api_receipt_elapsed_ns":receipt_ns,"outcome":outcome,
            "error_is_not_rollback":true}),
        )?;
        self.context
            .lock()
            .map_err(|_| io::Error::other("context poisoned"))?
            .take();
        Ok(())
    }
}
impl crate::ClusterRpcEvidenceObserver for Journal {
    fn record(&self, evidence: &crate::ClusterRpcEvidence) -> io::Result<()> {
        let context = self
            .context
            .lock()
            .map_err(|_| io::Error::other("context poisoned"))?
            .clone();
        if context.is_none() {
            return Err(io::Error::other("actual RPC has no live logical context"));
        }
        self.append(
            serde_json::json!({"kind":"actual_rpc","logical_context":context,"actual":evidence}),
        )
    }
}
fn check(ok: bool, reason: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(BicDbError::Cluster(reason.into()))
    }
}
fn sha_file(path: &Path) -> io::Result<String> {
    let mut input = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}
fn key_in(topology: &ClusterTopology, range: crate::RangeId, prefix: &str) -> String {
    (0..10_000)
        .map(|i| format!("{prefix}-{i}"))
        .find(|key| {
            let id = crate::cluster_event_record_id("items", key).unwrap();
            topology
                .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
                .unwrap()
                .id
                == range
        })
        .unwrap()
}
fn append(
    journal: &Journal,
    client: &crate::ClusterEventClient,
    key: &str,
    value: &serde_json::Value,
) -> Result<bool> {
    let id = journal.start("append", key, Some(value))?;
    let start = Instant::now();
    let result = client.append("items", key, value.clone());
    let elapsed = start.elapsed().as_nanos();
    journal.finish(id,match &result {
        Ok(inserted)=>serde_json::json!({"classification":"USER_ACK","inserted":inserted,"full_payload":value}),
        Err(error)=>serde_json::json!({"classification":"NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN","error":error.to_string()})
    },elapsed)?;
    result
}
fn read(
    journal: &Journal,
    client: &crate::ClusterEventClient,
    key: &str,
) -> Result<Option<serde_json::Value>> {
    let id = journal.start("get_strong", key, None)?;
    let start = Instant::now();
    let result = client.get_strong("items", key);
    let elapsed = start.elapsed().as_nanos();
    journal.finish(id,match &result {
        Ok(Some(value))=>serde_json::json!({"classification":"STRONG_PRESENT","full_payload":value}),
        Ok(None)=>serde_json::json!({"classification":"STRONG_ABSENT"}),
        Err(error)=>serde_json::json!({"classification":"NO_SUCCESSFUL_READ_NO_ABSENCE","error":error.to_string()})
    },elapsed)?;
    result
}
// Any API error is only an unsuccessful observation here. It is NOT
// reclassified as a typed unavailable/rollback; full wire errors remain in the
// journal and the terminal health gate still REQUIRES successful exact values.
fn no_false_successful_value(
    result: &Result<Option<serde_json::Value>>,
    expected: &serde_json::Value,
) -> bool {
    match result {
        Ok(Some(value)) => value == expected,
        Ok(None) => false,
        Err(_) => true,
    }
}
fn open_data(controls: &mut [NativeMetadataControlHost]) -> Result<()> {
    let end = Instant::now() + Duration::from_secs(20);
    let mut opened = vec![false; controls.len()];
    while !opened.iter().all(|v| *v) {
        for (i, control) in controls.iter_mut().enumerate() {
            if !opened[i] {
                opened[i] = control.open_initial_data(&budget(1500)).is_ok();
            }
        }
        check(
            Instant::now() < end,
            "actual initial DATA setup did not complete",
        )?;
    }
    Ok(())
}
fn run(
    f: &Fixture,
    controls: &mut [NativeMetadataControlHost],
    configs: &[DistributionConfig],
    journal: &Arc<Journal>,
    output: &Path,
) -> Result<()> {
    // Bootstrap remains the genuine public authenticated call. Its exact raw
    // frame is not covered by EventClient's observer, and is labelled as such.
    journal.append(
        serde_json::json!({"kind":"bootstrap_intent","destination":node(0),
        "address":configs[0].node_address,"bootstrap_raw_frame_covered":false}),
    )?;
    let bootstrap = crate::TcpClusterBootstrapClient::new(
        configs[0].cluster_id.clone(),
        node(0),
        f.networks[0].clone(),
    )?;
    let snapshot = bootstrap.fetch_snapshot(&node(0), &configs[0].node_address)?;
    journal.append(
        serde_json::json!({"kind":"bootstrap_reply","topology":snapshot.topology,
        "bootstrap_raw_frame_covered":false,"data_not_authority":true}),
    )?;
    let topology = snapshot.topology;
    let client = crate::ClusterEventClient::new(&configs[0], topology.clone())?
        .with_request_timeout(Duration::from_secs(2))?
        .with_rpc_evidence_observer(journal.clone(), crate::ClusterRpcEvidenceLimits::default())?;
    let range = topology
        .ranges
        .values()
        .find(|r| r.leader == node(0))
        .unwrap()
        .clone();
    let old = key_in(&topology, range.id, "origin-old-ack");
    let failed = key_in(&topology, range.id, "origin-injected-unknown");
    let next = key_in(&topology, range.id, "origin-next-ack");
    let old_value =
        serde_json::json!({"old":"entire external ACK","nested":[17,null,{"keep":true}]});
    let failed_value =
        serde_json::json!({"committed":"forced before origin WAL","nested":[null,{},[2,3]]});
    let next_value =
        serde_json::json!({"next":"requires autonomous native tail recovery","nested":[true,null]});
    check(
        append(journal, &client, &old, &old_value)?,
        "first external ACK missing",
    )?;
    check(
        read(journal, &client, &old)?.as_ref() == Some(&old_value),
        "first exact Strong missing",
    )?;
    let database = &f.hosts[0].seed.database;
    let before = {
        let db = database.read();
        let lineage = db.current_native_range_source_lineage(range.id)?;
        serde_json::json!({"native_R":lineage.range_index(),"H0":lineage.logical_genesis_sha256(),
            "H":lineage.logical_history_sha256(),"typed_local_B":lineage.actual_local_visibility_sequence(),
            "prefix":db.native_range_prefix_observation(range.id)?})
    };
    let old_r = before["native_R"].as_u64().unwrap();
    let old_command = f.hosts[0]
        ._service
        .lock()
        .probe_range_write_for_peer(&range, &node(0), old_r)?
        .entry
        .ok_or_else(|| BicDbError::Cluster("actual old ACK command missing".into()))?
        .command;
    {
        let db = database.read();
        let lineage = db.current_native_range_source_lineage(range.id)?;
        db.require_native_command_materialized(&lineage, &old_command)?;
    }
    journal.append(
        serde_json::json!({"kind":"old_ack_native_command_correlation",
        "event_id":crate::cluster_event_record_id("items",&old)?,"full_payload":old_value,
        "actual_command":old_command,"native_before":before}),
    )?;
    let arm: crate::db::NativeOriginFaultArm = database
        .read()
        .test_arm_native_origin_pre_wal_fault(range.id, "items", &failed)?;
    let probe: Arc<crate::db::NativeOriginFaultProbe> = arm.observation();
    journal.append(
        serde_json::json!({"kind":"fault_armed","actual_before":before,
        "probe":probe.snapshot(),"full_target_payload":failed_value}),
    )?;
    // The public client may retry. The observer retains every physical attempt;
    // later failures cannot overwrite the first actual origin receipt.
    let unknown = append(journal, &client, &failed, &failed_value);
    let end = Instant::now() + Duration::from_secs(6);
    let fault = loop {
        let snapshot = probe.snapshot();
        if snapshot["app_span_ended"] == true {
            break snapshot;
        }
        check(
            Instant::now() < end,
            "actual origin APP handler EXIT was not observed",
        )?;
        std::thread::sleep(Duration::from_millis(2));
    };
    journal.append(serde_json::json!({"kind":"actual_origin_exit","probe":fault,
        "api_error":unknown.as_ref().err().map(ToString::to_string),"no_ack_inferred_from_probe":true}))?;
    check(
        unknown.is_err(),
        "faulted public call unexpectedly ACKed; inspect full ledger",
    )?;
    check(
        fault["fired"] == true
            && fault["core_result"] == "commit_outcome_unknown"
            && fault["release_callback_returned"] == true
            && fault["release_callback_ok"] == true
            && fault["normal_app_handler_exit"] == true
            && fault["panicked"] == false
            && fault["diagnostic_contention"] == false
            && fault["unknown_sequence"].is_null()
            && fault["actual_transaction_id"] == fault["unknown_transaction_id"],
        "actual fault/Unknown/release/normal EXIT prerequisites failed",
    )?;
    let entry = f.hosts[0]
        ._service
        .lock()
        .probe_range_write_for_peer(&range, &node(0), old_r + 1)?
        .entry
        .ok_or_else(|| BicDbError::Cluster("actual admitted command not retained".into()))?;
    journal.append(
        serde_json::json!({"kind":"actual_admitted_command_correlation",
        "event_id":crate::cluster_event_record_id("items",&failed)?,"full_payload":failed_value,
        "actual_entry":entry,"probe":fault}),
    )?;
    check(
        entry.command.command_id == fault["actual_ticket_command_id"].as_str().unwrap(),
        "actual ticket/command identity differs",
    )?;
    check(
        matches!(
            entry.state,
            crate::RangeWriteState::QuorumCommitted | crate::RangeWriteState::Applied
        ),
        "actual CQ was not retained",
    )?;
    let origin = database.read().native_range_prefix_observation(range.id)?;
    journal.append(
        serde_json::json!({"kind":"actual_origin_prefix_after_handler_exit","origin":origin}),
    )?;
    check(
        (origin.native_applied_index == Some(old_r)
            && origin.pending_command_index == Some(old_r + 1)
            && origin.pending_command_id.as_deref() == Some(entry.command.command_id.as_str())
            && origin.materialized_pending_index.is_none())
            || origin.native_applied_index.is_some_and(|r| r >= old_r + 1),
        "actual origin prefix neither retained the intended Pending nor made genuine progress",
    )?;
    let mut physical = Vec::new();
    let mut materialized = 0;
    for (i, host) in f.hosts.iter().enumerate() {
        if node(i) == range.leader {
            continue;
        }
        // Collect BOTH actual peers before asserting. The DB guard ends before
        // any diagnostic fsync; a failed prerequisite is retained, not skipped.
        let actual = (|| -> Result<serde_json::Value> {
            let db = host.seed.database.read();
            let lineage = db.current_native_range_source_lineage(range.id)?;
            db.require_native_command_materialized(&lineage, &entry.command)?;
            db.require_native_command_materialized(&lineage, &old_command)?;
            let record = db
                .get(
                    crate::CLUSTER_EVENTS_COLLECTION,
                    &crate::cluster_event_record_id("items", &failed)?,
                )?
                .ok_or_else(|| {
                    BicDbError::Cluster("actual peer materialized row missing".into())
                })?;
            let previous = db
                .get(
                    crate::CLUSTER_EVENTS_COLLECTION,
                    &crate::cluster_event_record_id("items", &old)?,
                )?
                .ok_or_else(|| BicDbError::Cluster("actual old ACK peer row missing".into()))?;
            check(
                record.metadata == failed_value && previous.metadata == old_value,
                "actual peer full committed/old ACK payload differs",
            )?;
            Ok(serde_json::json!({"native_R":lineage.range_index(),
                "H0":lineage.logical_genesis_sha256(),"H":lineage.logical_history_sha256(),
                "typed_local_B":lineage.actual_local_visibility_sequence(),
                "full_record":record,"old_ack_full_record":previous}))
        })();
        match actual {
            Ok(facts) => {materialized += 1;physical.push(serde_json::json!({"node":node(i),"verified_actual_native":facts}));}
            Err(error) => physical.push(serde_json::json!({"node":node(i),"actual_materialization_error":error.to_string()})),
        }
    }
    journal.append(serde_json::json!({"kind":"actual_native_cut","origin":origin,
        "actual_command":entry.command,"raw_state":entry.state,"peer_native_materialization":physical,
        "native_proof_not_inferred_from_raw_counters":true}))?;
    check(
        materialized == 2,
        "two actual peer native materialization prerequisites failed",
    )?;
    // Record BOTH actual public reads before safety assertions. An error does
    // not prove rollback/absence; complete typed wire errors are in the journal.
    let old_after = read(journal, &client, &old);
    let committed_after = read(journal, &client, &failed);
    check(
        no_false_successful_value(&old_after, &old_value)
            && no_false_successful_value(&committed_after, &failed_value),
        "successful Strong returned false absence or wrong acknowledged/committed value",
    )?;
    let health_start = Instant::now();
    let health_end = health_start + Duration::from_secs(20);
    let mut healthy = false;
    let mut rounds = 0;
    while Instant::now() < health_end {
        rounds += 1;
        let old_read = read(journal, &client, &old);
        let committed_read = read(journal, &client, &failed);
        let next_ack = append(journal, &client, &next, &next_value);
        let next_read = read(journal, &client, &next);
        let receipt = health_start.elapsed().as_nanos();
        journal.append(serde_json::json!({"kind":"health_round","round":rounds,
            "receipt_elapsed_ns":receipt,"health_limit_ns":20_000_000_000u64,
            "old_exact":old_read.as_ref().is_ok_and(|v|v.as_ref()==Some(&old_value)),
            "committed_exact":committed_read.as_ref().is_ok_and(|v|v.as_ref()==Some(&failed_value)),
            "next_user_ack":next_ack.is_ok(),"next_exact":next_read.as_ref().is_ok_and(|v|v.as_ref()==Some(&next_value))}))?;
        check(
            no_false_successful_value(&old_read, &old_value)
                && no_false_successful_value(&committed_read, &failed_value),
            "post-fault Strong false absence/wrong value",
        )?;
        healthy = receipt <= 20_000_000_000
            && old_read
                .as_ref()
                .is_ok_and(|v| v.as_ref() == Some(&old_value))
            && committed_read
                .as_ref()
                .is_ok_and(|v| v.as_ref() == Some(&failed_value))
            && next_ack.is_ok()
            && next_read
                .as_ref()
                .is_ok_and(|v| v.as_ref() == Some(&next_value));
        if healthy {
            break;
        }
        // Supported live maintenance only, after BOTH reads and the new write
        // were actually attempted. Its finite work cannot starve first dispatch.
        for control in controls.iter_mut() {
            if health_end.saturating_duration_since(Instant::now()) < Duration::from_secs(8) {
                break;
            }
            let token = CancellationToken::uncancelable()
                .child_with_deadline(health_end.min(Instant::now() + Duration::from_millis(1250)));
            let status = control.metadata_status(&token);
            if status
                .as_ref()
                .is_ok_and(|s| s.role == crate::MetadataConsensusRole::Leader)
            {
                let report = control.maintain_joins(&token);
                journal.append(serde_json::json!({"kind":"supported_metadata_maintenance","ok":report.is_ok(),"error":report.err().map(|e|e.to_string())}))?;
            }
            let token = CancellationToken::uncancelable()
                .child_with_deadline(health_end.min(Instant::now() + Duration::from_secs(5)));
            let refresh = control.refresh_initial_data_registry(&token);
            journal.append(serde_json::json!({"kind":"supported_registry_maintenance","ok":refresh.is_ok(),"error":refresh.err().map(|e|e.to_string())}))?;
        }
        if health_end.saturating_duration_since(Instant::now()) < Duration::from_secs(8) {
            break;
        }
    }
    crate::storage::write_atomic(
        &output.join("terminal.json"),
        &serde_json::to_vec_pretty(&serde_json::json!({
        "bounded_autonomous_repair_and_next_ack":healthy,"rounds":rounds,
        "first_actual_origin_error":fault,"unknown_api_call_is_not_rollback":true,
        "no_manual_recovery_or_counter_reset":true,"physical_qualification":false,
        "health_elapsed_ns":health_start.elapsed().as_nanos(),"health_limit_ns":20_000_000_000u64}))?,
        true,
    )?;
    check(healthy,"REQUIRED autonomous committed-tail repair + next ACK/Strong unavailable; baseline remains RED")
}

#[test]
fn native_initial_data_actual_rpc_post_admission_fault_requires_autonomous_tail_recovery() {
    let (mut f, mut controls, configs) = actual_initial_metadata_controls();
    f._root.disable_cleanup(true);
    let output = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| f._root.path().join("public-diagnostics"))
        .join(format!("native-origin-pre-wal-{}", Uuid::now_v7()));
    let journal = Arc::new(Journal::create(&output.join("journal.jsonl")).unwrap());
    let exe = std::env::current_exe().unwrap();
    journal.append(serde_json::json!({"kind":"execution_identity",
        "source":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),"test_executable_sha256":sha_file(&exe).unwrap(),
        "test_executable_bytes":std::fs::metadata(exe).unwrap().len(),"component":"3 actual NativeHosts/RF3/TLS; not CLI processes or VMs",
        "private_original_root":f._root.path(),"portable_native_db_custody":false,
        "no_engine_reopen_or_repair":true})).unwrap();
    let process_parent = CancellationToken::uncancelable();
    let mut supervisors = Vec::new();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
        open_data(&mut controls)?;
        for control in &controls {
            supervisors.push(control.start_automatic_metadata(&process_parent)?);
        }
        run(&f, &mut controls, &configs, &journal, &output)
    }));
    let mut cleanup_errors = Vec::new();
    for supervisor in &mut supervisors {
        if let Err(e) = supervisor.stop(&budget(5000)) {
            cleanup_errors.push(e.to_string());
        }
    }
    for control in &mut controls {
        if let Err(e) = control.stop(&budget(5000)) {
            cleanup_errors.push(e.to_string());
        }
    }
    let first_error = match &result {
        Ok(Err(e)) => Some(e.to_string()),
        Err(_) => Some("actual test panic before successful terminal gate".into()),
        _ => None,
    };
    journal.append(serde_json::json!({"kind":"test_terminal_after_owned_stop_attempts","first_error":first_error,
        "cleanup_errors":cleanup_errors,"original_root_retained_path_only":f._root.path(),
        "portable_native_db_custody":false,"no_engine_reopen_or_repair":true})).unwrap();
    eprintln!("NATIVE_ORIGIN_FAULT_EVIDENCE {}", output.display());
    assert!(
        cleanup_errors.is_empty(),
        "actual owned cleanup failed: {cleanup_errors:?}"
    );
    match result {
        Ok(value) => value.unwrap(),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[test]
fn native_initial_data_origin_fault_arm_drop_revokes_retained_probe_without_erasing_successor() {
    let f = Fixture::new();
    let db = f.hosts[0].seed.database.read();
    let range = f.hosts[0].seed.spec.initial_ranges[0].id;
    let operation = crate::ClusterEventOperation::Append {
        namespace: "items".into(),
        key: "arm-owner-only".into(),
        value: serde_json::json!({"never_dispatched":true}),
    };
    let arm = db
        .test_arm_native_origin_pre_wal_fault(range, "items", "arm-owner-only")
        .unwrap();
    let retained = arm.observation();
    let span = db
        .test_begin_native_origin_app_span(range, &operation)
        .unwrap();
    drop(db);
    drop(arm);
    // Neither a retained observer nor an unfinished span can retain arm grant.
    assert!(!retained.snapshot()["armed"].as_bool().unwrap());
    assert!(f.hosts[0]
        .seed
        .database
        .read()
        .test_begin_native_origin_app_span(range, &operation)
        .is_none());
    let successor = f.hosts[0]
        .seed
        .database
        .read()
        .test_arm_native_origin_pre_wal_fault(range, "items", "arm-owner-only")
        .unwrap();
    let next = successor.observation();
    drop(span);
    // An old span must not clear a later actual registry slot (no Arc ABA).
    assert!(next.snapshot()["armed"].as_bool().unwrap());
    drop(successor);
    assert!(!next.snapshot()["armed"].as_bool().unwrap());
    assert!(!retained.snapshot()["fired"].as_bool().unwrap());
    assert!(!next.snapshot()["fired"].as_bool().unwrap());
}
