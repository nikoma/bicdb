//! FAILURE ONLY: bounded command/process diagnostics after actual kill/wait.
//! Explicit JSON/stdio allowlist; never recurse through DB or TLS trees.
use super::*;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Write},
};
const MAX_FILE: u64 = 8 * 1024 * 1024;
const MAX_TOTAL: u64 = 64 * 1024 * 1024;
const MAX_FILES: usize = 160;
fn io_error(e: impl Into<String>) -> std::io::Error {
    std::io::Error::other(e.into())
}
fn status(value: &std::process::ExitStatus) -> Value {
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        value.signal()
    };
    #[cfg(not(unix))]
    let signal: Option<i32> = None;
    serde_json::json!({"success":value.success(),"code":value.code(),"signal":signal,"display":value.to_string()})
}
impl Processes {
    pub(super) fn spawn_recorded(&mut self, command: &mut Command) -> std::io::Result<Child> {
        let invocation = serde_json::json!({"program":command.get_program().to_string_lossy(),"args":command.get_args().map(|a|a.to_string_lossy().into_owned()).collect::<Vec<_>>(),"cwd":command.get_current_dir().map(|p|p.to_string_lossy().into_owned()),"credential_file_contents_recorded":false});
        let child = command.spawn()?;
        self.spawn_records
            .push(serde_json::json!({"pid":child.id(),"invocation":invocation}));
        Ok(child)
    }
    pub(super) fn record_prior_exit(&mut self, pid: u32, value: &std::process::ExitStatus) {
        self.prior_exits.push(serde_json::json!({"pid":pid,"exit":status(value),"observed_by":"explicit-test-kill-and-wait-before-restart"}));
    }
    pub(super) fn stop_children_for_drop(&mut self) -> Vec<Value> {
        self.children.iter_mut().enumerate().map(|(slot,child)| {
            let before=child.try_wait();
            let before_json=match &before {Ok(Some(s))=>serde_json::json!({"exited":true,"status":status(s)}),Ok(None)=>serde_json::json!({"exited":false}),Err(e)=>serde_json::json!({"try_wait_error":e.to_string()})};
            let kill_attempted=!matches!(before,Ok(Some(_)));
            let kill_error=if kill_attempted {child.kill().err().map(|e|e.to_string())} else {None};
            // Always call actual wait, including an already-exited cached child.
            let waited=child.wait();
            let (wait_ok,exit,wait_error)=match waited {Ok(s)=>(true,Some(status(&s)),None),Err(e)=>(false,None,Some(e.to_string()))};
            serde_json::json!({"slot":slot,"pid":child.id(),"before_kill":before_json,"kill_attempted":kill_attempted,"kill_error":kill_error,"wait_ok":wait_ok,"exit":exit,"wait_error":wait_error})
        }).collect()
    }
    pub(super) fn capture_cli_failure(&self, exits: &[Value]) {
        let Some(destination) = std::env::var_os("BICDB_TEST_CLI_EVIDENCE_ROOT") else {
            return;
        };
        let Some(source) = self.root.as_ref() else {
            return;
        };
        let destination = PathBuf::from(destination)
            .join(format!("native-operator-cli-{}", uuid::Uuid::now_v7()));
        let result = (|| -> std::io::Result<Value> {
            fs::create_dir_all(&destination)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&destination, fs::Permissions::from_mode(0o700))?;
            }
            if !exits.iter().all(|e| e["wait_ok"] == true) {
                return Err(io_error(
                    "not every child was actually waited; refuse live process file capture",
                ));
            }
            let source = source.path().canonicalize()?;
            if destination.canonicalize()?.starts_with(&source) {
                return Err(io_error("custody destination is inside source tree"));
            }
            let mut allowed = vec![
                PathBuf::from("common-plan.json"),
                PathBuf::from("node4-killed-metadata.json"),
                PathBuf::from("initial-data-external-ledger.jsonl"),
            ];
            for i in 0..6 {
                for extension in ["stderr", "stdout", "status", "command"] {
                    allowed.push(format!("node{i}.{extension}").into());
                }
                for filename in [
                    "native-unassigned-learner.json",
                    "native-bootstrap-seed.json",
                    "cluster-topology.json",
                    "native-initial-metadata/cluster-metadata-consensus.json",
                    "native-learner-metadata/cluster-metadata-consensus.json",
                    "native-learner-metadata/native-learner-metadata-original-c.json",
                ] {
                    allowed.push(PathBuf::from(format!("db{i}")).join(filename));
                }
            }
            for filename in [
                "node4-cold.stderr",
                "node4-cold.stdout",
                "node4-promoted-cold.stderr",
                "node4-promoted-cold.stdout",
                "node4-promoted-kill-state.json",
                "node4-promoted-before.json",
                "node4.promoted-pre-kill.status",
                "node4.pre-kill.command",
                "node4.pre-kill.status",
                "wrong-address.command",
                "wrong-address.status",
                "fresh-over-existing.command",
                "fresh-over-existing.status",
                "other.command",
                "other.status",
                "replacement.command",
                "replacement.status",
            ] {
                allowed.push(filename.into());
            }
            allowed.sort();
            allowed.dedup();
            if allowed.len() > MAX_FILES {
                return Err(io_error("CLI allowlist bound exceeded"));
            }
            let mut files = Vec::new();
            let mut missing = Vec::new();
            let mut refused = Vec::new();
            let mut total = 0;
            for relative in allowed {
                let actual = source.join(&relative);
                let output = destination.join("files").join(&relative);
                if !actual.exists() {
                    missing.push(relative);
                    continue;
                }
                let result = copy_one(&actual, &output, &relative, &mut total);
                match result {
                    Ok(entry) => files.push(entry),
                    Err(e) => {
                        let cleanup_error = if output.exists() {
                            fs::remove_file(&output).err().map(|e| e.to_string())
                        } else {
                            None
                        };
                        refused.push(serde_json::json!({"path":relative,"error":e.to_string(),"partial_copy_cleanup_error":cleanup_error,"partial_copy_still_present":output.exists()}))
                    }
                }
            }
            sync_directories(&destination)?;
            Ok(
                serde_json::json!({"version":1,"scope":"failure-only stopped CLI process diagnostics and explicit saved-state JSON allowlist","status":if refused.is_empty(){"captured_allowlist_after_actual_wait"}else{"partial_refused_files"},"source_revision":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),"workflow_sha_not_source_authority":std::env::var("GITHUB_SHA").ok(),"package_version":env!("CARGO_PKG_VERSION"),"test":std::thread::current().name(),"actual_children_waited":exits,"prior_test_exits":self.prior_exits,"actual_spawn_commands_paths_only":self.spawn_records,"no_db_tree_capture":true,"no_tls_key_or_certificate_file_capture":true,"no_engine_open_repair_or_state_fabrication":true,"copied_files_fsynced":true,"total_bytes":total,"files":files,"missing_allowlisted_paths":missing,"refused_allowlisted_paths":refused}),
            )
        })();
        let manifest = match result {
            Ok(v) => v,
            Err(e) => {
                serde_json::json!({"version":1,"status":"capture_refused","error":e.to_string(),"actual_children_waited":exits,"actual_spawn_commands_paths_only":self.spawn_records,"prior_test_exits":self.prior_exits,"no_db_tree_capture":true,"no_tls_key_or_certificate_file_capture":true})
            }
        };
        let write = (|| -> std::io::Result<String> {
            let bytes =
                serde_json::to_vec_pretty(&manifest).map_err(|e| io_error(e.to_string()))?;
            let file = destination.join("manifest.json");
            let mut out = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&file)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                out.set_permissions(fs::Permissions::from_mode(0o600))?;
            }
            out.write_all(&bytes)?;
            out.sync_all()?;
            fs::File::open(&destination)?.sync_all()?;
            Ok(hex::encode(Sha256::digest(&bytes)))
        })();
        match write {
            Ok(sha) => eprintln!(
                "NATIVE_CLI_FAILURE_CUSTODY path={} manifest_sha256={sha} status={}",
                destination.display(),
                manifest["status"]
            ),
            Err(e) => eprintln!(
                "NATIVE_CLI_FAILURE_CUSTODY_REFUSED path={} error={e}",
                destination.display()
            ),
        }
    }
}
fn copy_one(
    source: &Path,
    destination: &Path,
    relative: &Path,
    total: &mut u64,
) -> std::io::Result<Value> {
    // Every path is an exact hard-coded allowlist entry. Refuse symlinks in
    // every ancestor so a metadata path cannot escape into a TLS/DB tree.
    for ancestor in source.ancestors() {
        if fs::symlink_metadata(ancestor)?.file_type().is_symlink() {
            return Err(io_error("refuse symlink in CLI diagnostic source"));
        }
    }
    let before = fs::symlink_metadata(source)?;
    if !before.is_file()
        || before.len() > MAX_FILE
        || total
            .checked_add(before.len())
            .is_none_or(|n| n > MAX_TOTAL)
    {
        return Err(io_error("CLI diagnostic file/total bound exceeded"));
    }
    fs::create_dir_all(
        destination
            .parent()
            .ok_or_else(|| io_error("missing custody parent"))?,
    )?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(before.len())
            .map_err(|_| io_error("CLI file length does not fit memory bound"))?,
    );
    fs::File::open(source)?
        .take(MAX_FILE + 1)
        .read_to_end(&mut bytes)?;
    let copied = bytes.len() as u64;
    if copied > MAX_FILE || total.checked_add(copied).is_none_or(|n| n > MAX_TOTAL) {
        return Err(io_error("CLI diagnostic source grew beyond bound"));
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| {
        io_error("allowlisted diagnostic is not text; refuse possible binary credential/DB payload")
    })?;
    if text.contains("PRIVATE KEY-----") {
        return Err(io_error(
            "refuse private-key material in allowlisted diagnostic",
        ));
    }
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        output.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    output.write_all(&bytes)?;
    output.sync_all()?;
    let after = fs::metadata(source)?;
    if copied != before.len()
        || before.len() != after.len()
        || before.modified()? != after.modified()?
    {
        return Err(io_error("stopped CLI source changed during copy"));
    }
    *total += copied;
    Ok(
        serde_json::json!({"path":Path::new("files").join(relative),"bytes":copied,"sha256":hex::encode(Sha256::digest(&bytes))}),
    )
}
fn sync_directories(path: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_directories(&entry.path())?;
        }
    }
    fs::File::open(path)?.sync_all()
}
