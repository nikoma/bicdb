use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

fn assess(path: &Path, check: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_bicdb"));
    command
        .env_remove("BICDB_RUNTIME_MODE")
        .args(["oracle", "assess"])
        .arg(path)
        .arg("--json");
    if check {
        command.arg("--fail-on-blockers");
    }
    command.output().unwrap()
}

#[test]
fn cli_separates_inventory_success_from_blocker_checks_and_readiness() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source.sql");
    fs::write(&source, "CREATE PACKAGE demo IS PROCEDURE run_job; END;\nBEGIN EXECUTE IMMEDIATE 'private-sample-secret'; END;").unwrap();
    let output = assess(&source, false);
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["migration_ready"], false);
    assert_eq!(report["runtime_assessment_required"], true);
    assert_eq!(report["sources"][0]["objects"][0]["name"], "DEMO");
    let output = assess(&source, true);
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("private-sample-secret"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("private-sample-secret"));
    fs::write(&source, "SELECT 'PACKAGE', \"ROWNUM\" FROM DUAL;").unwrap();
    let output = assess(&source, true);
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["sources"][0]["findings"], serde_json::json!([]));
    assert_eq!(report["migration_ready"], false);
    assert_eq!(
        fs::read_dir(root.path()).unwrap().count(),
        1,
        "assessment must not create database files"
    );
}

#[test]
fn cli_reports_incomplete_sources_and_rejects_nonfiles_and_size_overflow() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("broken.sql");
    fs::write(&source, "SELECT 'private-unterminated-literal").unwrap();
    let output = assess(&source, true);
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["sources"][0]["lexical_scan_complete"], false);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("private-unterminated-literal"));
    assert!(!assess(root.path(), true).status.success());
    let oversized = root.path().join("large.sql");
    fs::File::create(&oversized)
        .unwrap()
        .set_len(16 * 1024 * 1024 + 1)
        .unwrap();
    let output = assess(&oversized, true);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("16 MiB"));
}
