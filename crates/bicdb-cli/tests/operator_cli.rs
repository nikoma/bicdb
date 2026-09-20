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
