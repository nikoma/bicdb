use bicdb_app_runtime::{encoded_package_byte_limit, ApplicationHostConfig, MAX_MODULE_NAME_BYTES};
use std::{fs::File, io::Write, process::Command};

#[test]
fn every_package_ingestion_command_checks_file_size_before_decoding() {
    let directory = tempfile::tempdir().unwrap();
    let package = directory.path().join("oversized.bicapp");
    let limit =
        ApplicationHostConfig::new(directory.path().join("packages"), "test").max_package_bytes;
    let mut file = File::create(&package).unwrap();
    file.write_all(b"not JSON").unwrap();
    // Sparse length proves pre-decode rejection without allocating/writing a
    // multi-gigabyte test payload or consuming that much disk space.
    file.set_len(encoded_package_byte_limit(limit).unwrap() + 1)
        .unwrap();
    drop(file);
    for action in [
        "install",
        "stage",
        "validate",
        "verify-signature",
        "upgrade",
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
            .arg("app")
            .arg(directory.path().join(action))
            .arg(action)
            .arg(&package)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{action}");
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains("encoded package exceeds byte limit"),
            "{action}: {error}"
        );
    }
}

#[test]
fn cli_rejects_module_metadata_before_decoding_an_invalid_value() {
    let directory = tempfile::tempdir().unwrap();
    let package = directory.path().join("metadata.bicapp");
    std::fs::write(
        &package,
        format!(
            "{{\"modules\":{{\"{}\":!",
            "x".repeat(MAX_MODULE_NAME_BYTES + 1)
        ),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .arg("app")
        .arg(directory.path().join("db"))
        .arg("validate")
        .arg(package)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("module name must contain"), "{error}");
}
