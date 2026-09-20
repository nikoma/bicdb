use std::process::Command;

fn bicdb_cell_bin() -> &'static str {
    env!("CARGO_BIN_EXE_bicdb-cell")
}

#[test]
fn reduced_cell_binary_exposes_only_cell_lifecycle_commands() {
    let output = Command::new(bicdb_cell_bin())
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for command in [
        "serve",
        "verify",
        "manifest-sign",
        "volume-bind",
        "key-public",
        "digest",
    ] {
        assert!(stdout.contains(command), "missing {command} command");
    }
    for forbidden in ["cluster", "replication", "extension", "resp", "pgwire"] {
        assert!(
            !stdout.contains(forbidden),
            "reduced binary unexpectedly exposes {forbidden}"
        );
    }

    let serve = Command::new(bicdb_cell_bin())
        .args(["serve", "--help"])
        .output()
        .unwrap();
    assert!(serve.status.success());
    let serve_help = String::from_utf8_lossy(&serve.stdout);
    for capability in [
        "--release-policy",
        "--identity-policy",
        "--egress-policy",
        "--authorization-policy",
        "--feature-certification",
        "--fleet-trust-policy",
        "--fleet-activation-bundle",
        "--admission-trust-policy",
        "--admission-evidence-bundle",
        "--deployment-isolation-tier",
        "--previous-manifest",
        "--http-listen",
        "--http-tls-cert",
        "--http-tls-key",
    ] {
        assert!(
            serve_help.contains(capability),
            "cell serve is missing {capability}"
        );
    }
    for forbidden in ["--runtime-binary", "--host", "--port", "--database"] {
        assert!(
            !serve_help.contains(forbidden),
            "cell serve unexpectedly exposes {forbidden}"
        );
    }
}

fn minimum_serve_args() -> Vec<String> {
    vec![
        "serve".to_string(),
        "--manifest".to_string(),
        "missing-manifest".to_string(),
        "--volume".to_string(),
        "missing-volume".to_string(),
        "--expected-cell-id".to_string(),
        "018f7b30-4f4d-7b5c-a1f6-a183663e1240".to_string(),
        "--expected-volume-id".to_string(),
        "volume-a".to_string(),
        "--guest-image-digest".to_string(),
        format!("sha256:{}", "0".repeat(64)),
        "--key-file".to_string(),
        "missing-key".to_string(),
        "--artifact-root".to_string(),
        "missing-artifacts".to_string(),
    ]
}

#[test]
fn cell_application_listener_is_fail_closed_before_storage_open() {
    let mut remote = minimum_serve_args();
    remote.extend(["--http-listen".to_string(), "0.0.0.0:0".to_string()]);
    let output = Command::new(bicdb_cell_bin())
        .args(remote)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("a non-loopback cell application listener requires TLS"));

    let mut check_listener = minimum_serve_args();
    check_listener.extend([
        "--check".to_string(),
        "--http-listen".to_string(),
        "127.0.0.1:0".to_string(),
    ]);
    let output = Command::new(bicdb_cell_bin())
        .args(check_listener)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("--check cannot be combined with --http-listen"));

    let mut orphaned_tls = minimum_serve_args();
    orphaned_tls.extend([
        "--http-tls-cert".to_string(),
        "missing-cert".to_string(),
        "--http-tls-key".to_string(),
        "missing-key".to_string(),
    ]);
    let output = Command::new(bicdb_cell_bin())
        .args(orphaned_tls)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("cell application TLS credentials require --http-listen"));
}
