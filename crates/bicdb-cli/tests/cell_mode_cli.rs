use std::process::Command;

fn bicdb_bin() -> &'static str {
    env!("CARGO_BIN_EXE_bicdb")
}

#[test]
fn cell_mode_refuses_every_general_cli_construction_path_before_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("must-not-exist");
    let output = Command::new(bicdb_bin())
        .env("BICDB_RUNTIME_MODE", "cell")
        .args(["init", target.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("BICDB_RUNTIME_MODE=cell may launch only `bicdb cell ...`"));
    assert!(
        !target.exists(),
        "rejected general mode must not touch storage"
    );
}

#[test]
fn cell_mode_without_a_subcommand_selects_the_cell_environment_contract() {
    let output = Command::new(bicdb_bin())
        .env("BICDB_RUNTIME_MODE", "cell")
        .env_remove("BICDB_CELL_TRUSTED_KEYS")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("BICDB_CELL_TRUSTED_KEYS is required in cell runtime mode"));
}

#[test]
fn cell_serve_cannot_redirect_the_running_binary_measurement() {
    let output = Command::new(bicdb_bin())
        .args(["cell", "serve", "--help"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    for capability in [
        "--release-policy",
        "--identity-policy",
        "--egress-policy",
        "--admission-trust-policy",
        "--admission-evidence-bundle",
        "--deployment-isolation-tier",
        "--http-listen",
        "--http-tls-cert",
        "--http-tls-key",
    ] {
        assert!(help.contains(capability), "missing {capability}");
    }
    assert!(!help.contains("--runtime-binary"));
    assert!(!help.contains("--host"));
    assert!(!help.contains("--port"));
    assert!(!help.contains("--database"));
}
