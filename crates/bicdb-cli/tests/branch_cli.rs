//! `bicdb branch` on a cluster directory: create, list, diff, drop.
use std::process::Command;

fn bicdb(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args(args)
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), text)
}

#[test]
fn branch_commands_create_list_diff_and_drop() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("cluster");
    let root = root.to_str().unwrap();

    let (ok, text) = bicdb(&["branch", "create", root, "feature"]);
    assert!(ok, "{text}");
    let (ok, text) = bicdb(&["branch", "list", root, "--json"]);
    assert!(ok, "{text}");
    assert!(
        text.contains("\"feature\"") && text.contains("\"bicdb\""),
        "{text}"
    );
    let (ok, text) = bicdb(&["branch", "diff", root, "feature", "--json"]);
    assert!(ok, "{text}");
    let (ok, text) = bicdb(&["branch", "create", root, "feature"]);
    assert!(!ok && text.contains("already exists"), "{text}");
    let (ok, text) = bicdb(&["branch", "drop", root, "feature"]);
    assert!(ok, "{text}");
    let (ok, text) = bicdb(&["branch", "list", root, "--json"]);
    assert!(ok && !text.contains("\"feature\""), "{text}");
    let (ok, text) = bicdb(&["branch", "drop", root, "bicdb"]);
    assert!(!ok && text.contains("default database"), "{text}");
}
