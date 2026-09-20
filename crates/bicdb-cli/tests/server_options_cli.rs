use std::process::Command;

#[test]
fn postgres_server_exposes_the_rowid_registry_startup_control() {
    for command in ["serve", "serve-pg"] {
        let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
            .args([command, "--help"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(
            stdout.contains("--paged-rowid-registry <PAGED_ROWID_REGISTRY>"),
            "{command} help did not publish the startup control:\n{stdout}"
        );
    }
}

#[test]
fn channel_binding_require_is_wired_to_both_server_commands() {
    for command in ["serve", "serve-pg"] {
        let directory = tempfile::tempdir().unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
            .arg(command)
            .arg(directory.path())
            .args([
                "--require-auth",
                "--auth-method",
                "scram-sha-256",
                "--channel-binding",
                "require",
            ])
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(
            stderr.contains("channel_binding=require needs both tls_cert and tls_key"),
            "{command}: {stderr}"
        );
        let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
            .args([command, "--help"])
            .output()
            .unwrap();
        let help = String::from_utf8(output.stdout).unwrap();
        assert!(help.contains("--channel-binding"));
        assert!(help.contains("require, prefer, disable"));
    }
}
