use std::process::Command;

#[test]
fn password_environment_is_required_before_storage_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("must-not-exist");
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "user",
            "create",
            "test_user",
            "--password-env",
            "BICDB_TEST_MISSING_PASSWORD",
            "--path",
        ])
        .arg(&target)
        .env_remove("BICDB_TEST_MISSING_PASSWORD")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!target.exists());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("password environment variable is missing"));
}

#[test]
fn environment_password_creates_bound_login_without_disclosing_secret() {
    let directory = tempfile::tempdir().unwrap();
    let secret = "integration-only-not-a-deployment-secret";
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "user",
            "create",
            "test_user",
            "--password-env",
            "BICDB_TEST_PASSWORD",
            "--tenant",
            "test-org",
            "--path",
        ])
        .arg(directory.path())
        .env("BICDB_TEST_PASSWORD", secret)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
    let catalog = std::fs::read_to_string(directory.path().join("server_users.json")).unwrap();
    assert!(!catalog.contains(secret));
    assert!(catalog.contains("test-org"));
}

#[test]
fn empty_environment_password_is_rejected_without_storage_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("must-not-exist");
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "user",
            "create",
            "test_user",
            "--password-env",
            "BICDB_TEST_PASSWORD",
            "--path",
        ])
        .arg(&target)
        .env("BICDB_TEST_PASSWORD", "")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!target.exists());
}

#[test]
fn password_sources_are_mutually_exclusive() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("must-not-exist");
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "user",
            "create",
            "test_user",
            "--password",
            "test-only",
            "--password-env",
            "BICDB_TEST_PASSWORD",
            "--path",
        ])
        .arg(&target)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!target.exists());
}
