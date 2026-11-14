//! Real INITIAL metadata/TLS writer, then actual queued-hook exclusion. The
//! shared borrow is the same primitive used by Joined; not a Joined/R2 test.
use super::*;
use std::io::{Read, Write};
const CHILD_ENV: &str = "BICDB_NATIVE_BORROWED_HOOK_CHILD";
const MARKER: &str = "actual-hook-writer-queued.json";
fn durable(path: &std::path::Path, value: &serde_json::Value) {
    let bytes = serde_json::to_vec_pretty(value).unwrap();
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    std::fs::File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}
fn child(root: &std::path::Path) {
    let (f, mut controls, _configs) = actual_initial_metadata_controls();
    let until = Instant::now() + Duration::from_secs(20);
    let mut ready = [false; 3];
    while !ready.iter().all(|r| *r) {
        for (i, control) in controls.iter_mut().enumerate() {
            if !ready[i] {
                ready[i] = control.open_initial_data(&budget(1500)).is_ok();
            }
        }
        assert!(
            Instant::now() < until,
            "actual INITIAL DATA prerequisite incomplete: {ready:?}"
        );
    }
    let result = {
        let db = f.hosts[0].seed.database.read();
        db.test_original_writer_identity_with_queued_hook_writer(&root.join(MARKER))
    };
    let report = serde_json::json!({"actual_result":result.as_ref().ok(),"exact_error":result.as_ref().err().map(ToString::to_string),"actual_initial_data_completed":ready,"original_initial_open_budget_seconds":20,"actual_queued_control_not_application_or_Joined_acceptance":true});
    durable(&root.join("actual-hook-control-result.json"), &report);
    eprintln!("ACTUAL_NATIVE_BORROWED_HOOK_CONTROL {report}");
    let value = result.unwrap();
    assert_eq!(value["prerequisite_reached"], true);
    assert!(value["marker_error"].is_null());
    assert!(value["borrowed_validation_error"].is_null());
    assert_eq!(
        value["queued_observation"]["actual_writer_call_entered"],
        true
    );
    assert_eq!(
        value["queued_observation"]["actual_try_read_refused_while_original_read_live"],
        true
    );
    assert_eq!(
        value["queued_observation"]["own_writer_acquired_before_point_drop"],
        false
    );
    assert_eq!(value["own_writer_joined"], true);
    assert_eq!(value["own_writer_acquired_after_point_drop"], true);
    assert!(value["ordinary_identity_after_cleanup_error"].is_null());
    assert_eq!(value["original_policy_unchanged"], true);
}
#[test]
fn native_initial_real_writer_borrowed_identity_progresses_with_actual_queued_hook_writer() {
    if let Some(root) = std::env::var_os(CHILD_ENV) {
        child(std::path::Path::new(&root));
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let log_path = root.path().join("child.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let exact = std::thread::current().name().unwrap().to_owned();
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &exact, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, root.path())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let setup_until = Instant::now() + Duration::from_secs(20);
    let mut selected_started = None;
    let mut status = None;
    let mut timeout = None;
    loop {
        if let Some(done) = process.try_wait().unwrap() {
            status = Some(done);
            break;
        }
        if selected_started.is_none() && root.path().join(MARKER).is_file() {
            selected_started = Some(Instant::now());
        }
        if let Some(started) = selected_started {
            if started.elapsed() >= Duration::from_secs(5) {
                timeout =
                    Some("original actual queued-hook child exceeded5s; own child terminated");
                break;
            }
        } else if Instant::now() >= setup_until {
            timeout = Some(
                "actual installed/queued prerequisite not reached within original20s setup bound",
            );
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    if timeout.is_some() {
        process.kill().unwrap();
        status = Some(process.wait().unwrap());
    }
    let queued = std::fs::read(root.path().join(MARKER))
        .ok()
        .map(|b| serde_json::from_slice::<serde_json::Value>(&b).unwrap());
    let result = std::fs::read(root.path().join("actual-hook-control-result.json"))
        .ok()
        .map(|b| serde_json::from_slice::<serde_json::Value>(&b).unwrap());
    let validation_returned =
        std::fs::read(root.path().join("actual-hook-validation-returned.json"))
            .ok()
            .map(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).unwrap());
    let timeout_phase = timeout.map(|_| {
        if result.is_some() {
            "control result retained; later assertions or fixture cleanup/Drop; not borrowed-validation deadlock proof"
        } else if validation_returned.is_some() {
            "validation returned; point drop/own writer join/post-cleanup identity/result persistence unfinished"
        } else if queued.is_some() {
            "queued marker retained; borrowed validation or validation-marker persistence unfinished; cause unresolved"
        } else {
            "actual installed/queued prerequisite or marker persistence unfinished"
        }
    });
    let file = std::fs::File::open(&log_path).unwrap();
    let log_bytes = file.metadata().unwrap().len();
    let mut raw = Vec::new();
    Read::take(file, 16 * 1024 * 1024)
        .read_to_end(&mut raw)
        .unwrap();
    let logs = String::from_utf8_lossy(&raw);
    eprintln!(
        "ACTUAL_NATIVE_BORROWED_HOOK_PARENT {}",
        serde_json::json!({"actual_queued_marker":queued,"actual_result":result,"validation_returned":validation_returned,"timeout_phase":timeout_phase,"five_second_measurement_scope":"queued marker observed to child exit, including control cleanup and fixture Drop","child_status":status.map(|s|s.to_string()),"timeout":timeout,"queued_child_bound_seconds":5,"setup_bound_seconds":20,"child_log_bytes":log_bytes,"child_log_truncated":log_bytes>16*1024*1024,"no_process_outside_owned_child_terminated":true})
    );
    eprintln!("ACTUAL_NATIVE_BORROWED_HOOK_CHILD_LOG {logs}");
    assert!(timeout.is_none(), "{timeout:?}");
    assert!(status.unwrap().success());
    assert!(queued.is_some());
    assert!(result.is_some());
}
