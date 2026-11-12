//! Opt-in FAILURE-only complete-root custody. No engine open or reconciliation.
use super::*;
use std::{fs, io::Write};

impl Processes {
    pub(super) fn capture_cli_database_failure(&self, exits: &[Value]) {
        let Some(destination) = std::env::var_os("BICDB_TEST_CLI_DB_EVIDENCE_ROOT") else {
            return;
        };
        let Some(source) = &self.root else { return };
        let run = || -> std::io::Result<()> {
            // Every actual spawn must have a real current wait or an earlier
            // explicit kill+wait. A queued command/status is never a wait.
            let waited: std::collections::BTreeSet<u64> = exits
                .iter()
                .filter(|row| row["wait_ok"] == true)
                .chain(self.prior_exits.iter())
                .filter_map(|row| row["pid"].as_u64())
                .collect();
            if exits.len() != self.children.len()
                || exits.iter().any(|row| row["wait_ok"] != true)
                || self
                    .spawn_records
                    .iter()
                    .any(|row| row["pid"].as_u64().is_none_or(|pid| !waited.contains(&pid)))
            {
                return Err(std::io::Error::other(
                    "refuse DB custody before ALL actual child waits",
                ));
            }
            // Request lives outside source DB roots. Actual destination/root
            // validation and caps belong to the same locally tested helper.
            let mut request = tempfile::NamedTempFile::new()?;
            serde_json::to_writer(&mut request, &serde_json::json!({
                "version":1,"source_revision":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),
                "test":std::thread::current().name(),"fixture_root":source.path(),
                "owned_database_roots":self.owned_database_roots,
                "actual_children_waited":exits,"prior_test_exits":self.prior_exits,
                "actual_spawn_records":self.spawn_records,"destination_root":PathBuf::from(destination),
            })).map_err(std::io::Error::other)?;
            request.flush()?;
            let mut helper = Command::new("python3")
                .arg(
                    Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("tests/native_metadata_operator_cli/database_custody.py"),
                )
                .arg("--request")
                .arg(request.path())
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()?;
            // Helper caps <=120s; do not silently leave a copier after Drop.
            // OS-level read/fsync/kill wait can still be uncancellable; no hard
            // kernel-I/O deadline or original power-loss durability is claimed.
            let deadline = Instant::now() + Duration::from_secs(125);
            loop {
                if let Some(status) = helper.try_wait()? {
                    if !status.success() {
                        return Err(std::io::Error::other(format!(
                            "DB custody helper exited {status}; PARTIAL/quarantine only"
                        )));
                    }
                    return Ok(());
                }
                if Instant::now() >= deadline {
                    let killed = helper.kill();
                    let waited = helper.wait();
                    eprintln!("NATIVE_CLI_DATABASE_CUSTODY_PARTIAL helper_timeout=true killed={killed:?} waited={waited:?}; unpublished staging must remain quarantined");
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        if let Err(error) = run() {
            eprintln!("NATIVE_CLI_DATABASE_CUSTODY_PARTIAL error={error}; no complete custody or reconciliation claim");
        }
    }
}
