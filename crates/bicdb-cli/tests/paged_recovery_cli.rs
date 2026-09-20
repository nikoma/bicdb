use std::process::Command;

#[test]
fn paged_recovery_benchmark_uses_a_clean_probe_and_exports_evidence() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("fixture");
    let json_report = root.path().join("recovery.json");
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "bench",
            "paged-recovery",
            "--checkpointed-data-bytes",
            "32768",
            "--wal-bytes",
            "65536",
            "--record-bytes",
            "128",
            "--buffer-pool-bytes",
            "65536",
            "--page-size",
            "512",
            "--batch",
            "8",
            "--fsync",
            "false",
            "--sample-interval-ms",
            "1",
            "--max-recovery-ms",
            "60000",
            "--max-peak-rss-bytes",
            "1073741824",
            "--path",
        ])
        .arg(&database)
        .arg("--json-out")
        .arg(&json_report)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&json_report).unwrap()).unwrap();
    assert_eq!(
        report["format_version"],
        bicdb_bench::PAGED_RECOVERY_BENCH_FORMAT_VERSION
    );
    assert_eq!(report["mode"], "paged_recovery");
    assert_eq!(report["environment"]["cache_state"], "uncontrolled");
    assert_eq!(
        report["environment"]["executable_sha256"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    assert_eq!(report["checksum_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(report["probe"]["recovery"]["scan_passes"], 2);
    assert_eq!(report["probe"]["verified_checkpointed_records"], 3);
    assert_eq!(report["probe"]["verified_suffix_records"], 3);
    // The fixture closes every transaction before restart, so the open-time
    // freeze must absorb the whole outcome set and retain no exceptions.
    assert_eq!(
        report["probe"]["recovery"]["frozen_outcomes_at_open"],
        report["probe"]["recovery"]["transaction_outcomes"]
    );
    assert_eq!(report["probe"]["recovery"]["abort_exceptions_at_open"], 0);
    assert_eq!(report["passed"], true);

    let verification = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args(["bench", "paged-recovery-verify", "--report"])
        .arg(&json_report)
        .output()
        .unwrap();
    assert!(
        verification.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&verification.stdout),
        String::from_utf8_lossy(&verification.stderr)
    );
    assert!(String::from_utf8_lossy(&verification.stdout).contains("(PASS)"));

    #[cfg(unix)]
    {
        let symlink_report = root.path().join("recovery-link.json");
        std::os::unix::fs::symlink(&json_report, &symlink_report).unwrap();
        let linked = Command::new(env!("CARGO_BIN_EXE_bicdb"))
            .args(["bench", "paged-recovery-verify", "--report"])
            .arg(&symlink_report)
            .output()
            .unwrap();
        assert!(!linked.status.success());
        assert!(String::from_utf8_lossy(&linked.stderr).contains("not a symlink"));
    }
}

#[test]
fn paged_recovery_benchmark_refuses_an_existing_target() {
    let root = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "bench",
            "paged-recovery",
            "--wal-bytes",
            "4096",
            "--fsync",
            "false",
            "--path",
        ])
        .arg(root.path())
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("refuses existing path"));
}

#[test]
fn paged_recovery_benchmark_writes_failed_evidence_before_exiting() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("fixture");
    let json_report = root.path().join("failed-recovery.json");
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "bench",
            "paged-recovery",
            "--wal-bytes",
            "32768",
            "--record-bytes",
            "128",
            "--buffer-pool-bytes",
            "65536",
            "--page-size",
            "512",
            "--batch",
            "8",
            "--fsync",
            "false",
            "--max-peak-rss-bytes",
            "0",
            "--path",
        ])
        .arg(&database)
        .arg("--json-out")
        .arg(&json_report)
        .output()
        .unwrap();

    assert!(!output.status.success());
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(json_report).unwrap()).unwrap();
    assert_eq!(report["passed"], false);
    assert!(report["failures"]
        .as_array()
        .is_some_and(|failures| !failures.is_empty()));
    assert!(String::from_utf8_lossy(&output.stderr).contains("exceeded its release limits"));
}

#[test]
fn paged_recovery_release_evidence_fails_before_fixture_generation_when_incomplete() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("fixture");
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "bench",
            "paged-recovery",
            "--wal-bytes",
            "4096",
            "--fsync",
            "false",
            "--require-release-evidence",
            "--path",
        ])
        .arg(&database)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!database.exists());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("release-evidence preflight failed"));
    assert!(stderr.contains("fsync=true"));
    assert!(stderr.contains("source revision"));
    assert!(stderr.contains("cache state"));
}

#[test]
fn paged_recovery_verifier_rejects_tampered_evidence() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("fixture");
    let json_report = root.path().join("recovery.json");
    let tampered_report = root.path().join("tampered.json");
    let generated = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "bench",
            "paged-recovery",
            "--wal-bytes",
            "32768",
            "--record-bytes",
            "128",
            "--buffer-pool-bytes",
            "65536",
            "--page-size",
            "512",
            "--batch",
            "8",
            "--fsync",
            "false",
            "--path",
        ])
        .arg(&database)
        .arg("--json-out")
        .arg(&json_report)
        .output()
        .unwrap();
    assert!(generated.status.success());
    let mut report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(json_report).unwrap()).unwrap();
    report["probe"]["recovery"]["scan_passes"] = 1.into();
    std::fs::write(
        &tampered_report,
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();

    let verification = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args(["bench", "paged-recovery-verify", "--report"])
        .arg(&tampered_report)
        .output()
        .unwrap();
    assert!(!verification.status.success());
    assert!(String::from_utf8_lossy(&verification.stderr)
        .contains("outcome was not derived from its evidence"));
}
