use std::process::Command;

#[test]
fn developer_commands_follow_the_bench_feature() {
    for command in ["bench", "compat"] {
        let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
            .args([command, "--help"])
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            cfg!(feature = "bench"),
            "{command}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args(["serve-pg", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
}

#[cfg(feature = "bench")]
#[test]
fn optional_benchmark_build_runs_an_insert_workload() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "bench",
            "inserts",
            "--records",
            "3",
            "--batch-size",
            "3",
            "--path",
        ])
        .arg(directory.path().join("bench"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(feature = "bench")]
#[test]
fn comparison_command_requires_the_comparison_engines_feature() {
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args(["bench", "compare", "--help"])
        .output()
        .unwrap();
    assert_eq!(
        output.status.success(),
        cfg!(feature = "bench-comparison-engines")
    );
}
