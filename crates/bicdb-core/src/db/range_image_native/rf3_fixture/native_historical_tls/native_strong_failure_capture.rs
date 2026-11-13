//! Opt-in, test-only raw failure custody. No engine reopen/checkpoint/repair.
//! Copies are NOT labeled clean-close or an atomic power-loss snapshot.
use super::*;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};
const MAX_FILES: usize = 4096;
const MAX_FILE: u64 = 64 * 1024 * 1024;
const MAX_TOTAL: u64 = 256 * 1024 * 1024;
#[derive(serde::Serialize)]
struct Entry {
    path: String,
    bytes: u64,
    sha256: String,
    source: PathBuf,
}
fn error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}
fn digest(path: &Path) -> std::io::Result<String> {
    let mut input = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}
fn sensitive(path: &Path) -> bool {
    path.extension().and_then(|x| x.to_str()).is_some_and(|x| {
        matches!(
            x.to_ascii_lowercase().as_str(),
            "pem" | "key" | "pfx" | "p12" | "crt" | "csr"
        )
    }) || path
        .file_name()
        .and_then(|x| x.to_str())
        .is_some_and(|x| x.to_ascii_lowercase().contains("private"))
}
fn copy_tree(
    src: &Path,
    dst: &Path,
    relative: &Path,
    entries: &mut Vec<Entry>,
    total: &mut u64,
) -> std::io::Result<()> {
    if !src.exists() {
        return Err(error(format!("actual source missing: {}", src.display())));
    }
    let metadata = fs::symlink_metadata(src)?;
    if metadata.file_type().is_symlink() {
        return Err(error("refuse symlink in actual evidence tree"));
    }
    if sensitive(src) {
        return Err(error(
            "refuse credential-like filename in allowed evidence tree",
        ));
    }
    if metadata.is_dir() {
        fs::create_dir_all(dst)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(dst, fs::Permissions::from_mode(0o700))?;
        }
        let mut children = fs::read_dir(src)?.collect::<std::io::Result<Vec<_>>>()?;
        children.sort_by_key(|e| e.file_name());
        for child in children {
            copy_tree(
                &child.path(),
                &dst.join(child.file_name()),
                &relative.join(child.file_name()),
                entries,
                total,
            )?;
        }
        fs::File::open(dst)?.sync_all()?;
        return Ok(());
    }
    if !metadata.is_file()
        || metadata.len() > MAX_FILE
        || entries.len() >= MAX_FILES
        || total
            .checked_add(metadata.len())
            .is_none_or(|n| n > MAX_TOTAL)
    {
        return Err(error("actual failure custody bound exceeded"));
    }
    let mut input = fs::File::open(src)?;
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dst)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        output.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    let mut hash = Sha256::new();
    let mut copied = 0u64;
    let mut buf = [0u8; 65536];
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        copied = copied
            .checked_add(n as u64)
            .ok_or_else(|| error("copy overflow"))?;
        if copied > MAX_FILE || total.checked_add(copied).is_none_or(|n| n > MAX_TOTAL) {
            return Err(error("source grew beyond bound"));
        }
        output.write_all(&buf[..n])?;
        hash.update(&buf[..n]);
    }
    output.sync_all()?;
    let hash = hex::encode(hash.finalize());
    let after = fs::metadata(src)?;
    if copied != metadata.len()
        || after.len() != metadata.len()
        || after.modified()? != metadata.modified()?
        || digest(src)? != hash
    {
        return Err(error("actual source changed during capture"));
    }
    *total += copied;
    entries.push(Entry {
        path: relative.to_string_lossy().into_owned(),
        bytes: copied,
        sha256: hash,
        source: src.into(),
    });
    Ok(())
}
fn actual_file_set(
    root: &Path,
    out: &mut std::collections::BTreeSet<PathBuf>,
) -> std::io::Result<()> {
    let meta = fs::symlink_metadata(root)?;
    if meta.file_type().is_symlink() || sensitive(root) {
        return Err(error(
            "unexpected symlink/credential-like source during final file-set check",
        ));
    }
    if meta.is_file() {
        out.insert(root.to_owned());
    } else if meta.is_dir() {
        for entry in fs::read_dir(root)? {
            actual_file_set(&entry?.path(), out)?;
        }
    } else {
        return Err(error("unsupported evidence source file type"));
    }
    if out.len() > MAX_FILES {
        return Err(error("final source file-set bound exceeded"));
    }
    Ok(())
}
impl EnforcedNetwork {
    pub(super) fn capture_actual_failure_cut(&mut self) {
        let Some(root) = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT") else {
            return;
        };
        let root = PathBuf::from(root).join(format!("native-joint-cut-{}", Uuid::now_v7()));
        if let Err(e) = fs::create_dir_all(&root) {
            eprintln!("NATIVE_JOINT_CUSTODY_REFUSED mkdir={e}");
            return;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Err(error) = fs::set_permissions(&root, fs::Permissions::from_mode(0o700)) {
                eprintln!("NATIVE_JOINT_CUSTODY_REFUSED permissions={error}");
                return;
            }
        }
        let result = (|| -> std::io::Result<serde_json::Value> {
            let budget = CancellationToken::uncancelable()
                .child_with_deadline(Instant::now() + Duration::from_secs(10));
            // Stop AND drain actual listeners, not merely their accept thread.
            for server in &mut self.servers {
                server
                    .stop_and_drain(&budget)
                    .map_err(|e| error(e.to_string()))?;
            }
            while self
                .runtimes
                .values()
                .any(|r| r.test_active_context_count() != 0)
            {
                budget.check().map_err(|e| error(e.to_string()))?;
                std::thread::sleep(Duration::from_millis(2));
            }
            let metadata = self
                .metadata
                .values()
                .map(|m| {
                    m.try_lock()
                        .ok_or_else(|| error("actual metadata store still busy"))
                })
                .collect::<std::io::Result<Vec<_>>>()?;
            let _services = self
                .native
                .services
                .values()
                .map(|s| {
                    s.try_lock()
                        .ok_or_else(|| error("actual service still busy"))
                })
                .collect::<std::io::Result<Vec<_>>>()?;
            let databases = self
                .native
                .databases
                .values()
                .map(|db| {
                    db.try_write()
                        .ok_or_else(|| error("actual DB owner/callback still busy"))
                })
                .collect::<std::io::Result<Vec<_>>>()?;
            let _journals = databases
                .iter()
                .map(|db| {
                    db.try_native_image_journal()
                        .map_err(|e| error(e.to_string()))
                })
                .collect::<std::io::Result<Vec<_>>>()?;
            let source = self.native.root.path().canonicalize()?;
            let destination = root.canonicalize()?;
            if destination.starts_with(&source) {
                return Err(error("custody destination is inside actual source"));
            }
            let mut entries = Vec::new();
            let mut total = 0;
            copy_tree(
                &source,
                &root.join("databases"),
                Path::new("databases"),
                &mut entries,
                &mut total,
            )?;
            // Explicit actual metadata directories ONLY. Never copy the TLS root.
            for node in self.metadata.keys() {
                let name = format!("strong-meta-{node}");
                let source = self._keys.path().join(&name);
                copy_tree(
                    &source,
                    &root.join("metadata").join(&name),
                    &Path::new("metadata").join(name),
                    &mut entries,
                    &mut total,
                )?;
            }
            fs::File::open(root.join("metadata"))?.sync_all()?;
            let mut actual_files = std::collections::BTreeSet::new();
            actual_file_set(&source, &mut actual_files)?;
            for node in self.metadata.keys() {
                actual_file_set(
                    &self._keys.path().join(format!("strong-meta-{node}")),
                    &mut actual_files,
                )?;
            }
            if actual_files != entries.iter().map(|e| e.source.clone()).collect() {
                return Err(error("actual source file set changed during capture"));
            }
            for item in &entries {
                if digest(&item.source)? != item.sha256 {
                    return Err(error("source changed after complete copied-file pass"));
                }
            }
            let states=metadata.iter().map(|m| {let s=m.status();serde_json::json!({"term":s.current_term,"commit_index":s.commit_index,"last_index":s.last_log_index,"generation":s.topology_generation})}).collect::<Vec<_>>();
            Ok(
                serde_json::json!({"version":1,"status":"captured_observed_stable_with_live_engine_owners","source_revision":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),"workflow_sha_not_source_authority":std::env::var("GITHUB_SHA").ok(),"package_version":env!("CARGO_PKG_VERSION"),"test":std::thread::current().name(),"listeners_actually_drained":true,"db_service_metadata_and_native_journal_locks_held_during_copy":true,"engine_owners_remain_alive":true,"engine_reopened":false,"checkpoint_or_repair_invoked":false,"tls_private_root_excluded":true,"atomic_powerloss_snapshot_or_replay_verified":false,"copied_files_fsynced":true,"metadata":states,"bytes":total,"files":entries}),
            )
        })();
        let manifest = match result {
            Ok(m) => m,
            Err(e) => {
                serde_json::json!({"version":1,"status":"capture_refused_or_partial","error":e.to_string(),"not_verified_crash_snapshot":true,"tls_private_root_excluded":true})
            }
        };
        match serde_json::to_vec_pretty(&manifest)
            .map_err(|e| error(e.to_string()))
            .and_then(|bytes| {
                crate::storage::write_atomic(&root.join("manifest.json"), &bytes, true)
                    .map_err(|e| error(e.to_string()))?;
                fs::File::open(&root)?.sync_all()?;
                Ok(hex::encode(Sha256::digest(&bytes)))
            }) {
            Ok(hash) => eprintln!(
                "NATIVE_JOINT_FAILURE_CUSTODY path={} manifest_sha256={hash} status={}",
                root.display(),
                manifest["status"]
            ),
            Err(e) => eprintln!(
                "NATIVE_JOINT_FAILURE_CUSTODY_MANIFEST_FAILED path={} error={e}",
                root.display()
            ),
        }
    }
}
