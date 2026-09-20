use std::process::Command;

fn bicdb_bin() -> &'static str {
    env!("CARGO_BIN_EXE_bicdb")
}

#[test]
fn cli_help_lists_tui_global_option() {
    let output = Command::new(bicdb_bin()).arg("--help").output().unwrap();

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--tui [<PATH>]"));
    assert!(stdout.contains("--tui-key"));
    assert!(stdout.contains("--tui-key-env"));
}
