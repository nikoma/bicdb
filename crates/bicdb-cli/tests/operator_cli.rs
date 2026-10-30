use std::process::Command;

#[test]
fn operator_flags_are_available_on_both_server_commands_and_require_a_complete_configuration() {
    for command in ["serve", "serve-pg"] {
        let help = Command::new(env!("CARGO_BIN_EXE_bicdb"))
            .args([command, "--help"])
            .output()
            .unwrap();
        assert!(help.status.success());
        for flag in [
            "--operator-listen",
            "--operator-token-file",
            "--operator-actor",
        ] {
            assert!(String::from_utf8_lossy(&help.stdout).contains(flag));
        }
        for args in [
            vec!["--operator-listen", "127.0.0.1:5440"],
            vec!["--operator-actor", "operator"],
            vec!["--operator-token-file", "/nonexistent/test-credential"],
        ] {
            let directory = tempfile::tempdir().unwrap();
            let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
                .arg(command)
                .arg(directory.path().join("not-created"))
                .args(args)
                .output()
                .unwrap();
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr).contains("required"));
            assert!(!directory.path().join("not-created").exists());
        }
    }
}

#[test]
fn offline_verification_checks_sql_catalog_identity_as_well_as_physical_bytes() {
    let root = tempfile::tempdir().unwrap();
    {
        let mut db = bicdb_core::BicDb::open(root.path()).unwrap();
        bicdb_sql::SqlSession::new(&mut db).execute("CREATE TABLE original_type(id INT PRIMARY KEY); CREATE TABLE aliased_type(id INT PRIMARY KEY)").unwrap();
    }
    let healthy = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .arg("verify")
        .arg(root.path())
        .arg("--json")
        .output()
        .unwrap();
    assert!(
        healthy.status.success(),
        "{}",
        String::from_utf8_lossy(&healthy.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&healthy.stdout).unwrap();
    assert_eq!(report["catalog_identifiers_valid"], true);
    {
        let mut db = bicdb_core::BicDb::open(root.path()).unwrap();
        let original = db
            .get("__bicdb_pg_schema", "original_type")
            .unwrap()
            .unwrap();
        let mut aliased = db
            .get("__bicdb_pg_schema", "aliased_type")
            .unwrap()
            .unwrap()
            .as_ref()
            .clone();
        aliased.metadata["row_type_oid"] = original.metadata["row_type_oid"].clone();
        db.insert("__bicdb_pg_schema", aliased).unwrap();
    }
    let corrupt = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .arg("verify")
        .arg(root.path())
        .arg("--json")
        .output()
        .unwrap();
    assert!(!corrupt.status.success());
    let error = String::from_utf8_lossy(&corrupt.stderr);
    assert!(
        error.contains("pg_type") && error.contains("aliases"),
        "{error}"
    );
}
