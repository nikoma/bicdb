//! Bounded raw cut while actual owners stay alive. No engine open or repair.
use super::*;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MAX_FILES: usize = 4096;
const MAX_FILE: u64 = 64 * 1024 * 1024;
const MAX_TOTAL: u64 = 256 * 1024 * 1024;
/// How long a refused all-owner cut is repeated before it is reported.
const CUSTODY_CUT_WAIT: std::time::Duration = std::time::Duration::from_secs(2);
fn error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}
fn digest(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hash.update(&buf[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}
fn files(root: &Path, result: &mut BTreeMap<PathBuf, String>, bytes: &mut u64) -> io::Result<()> {
    let meta = fs::symlink_metadata(root)?;
    if meta.file_type().is_symlink() {
        return Err(error("symlink in actual evidence source"));
    }
    if meta.is_dir() {
        for entry in fs::read_dir(root)? {
            files(&entry?.path(), result, bytes)?;
        }
    } else if meta.is_file() {
        if meta.len() > MAX_FILE
            || result.len() >= MAX_FILES
            || bytes.checked_add(meta.len()).is_none_or(|n| n > MAX_TOTAL)
        {
            return Err(error("bounded actual source inventory limit exceeded"));
        }
        *bytes += meta.len();
        result.insert(root.canonicalize()?, digest(root)?);
    } else {
        return Err(error("non-file in actual evidence source"));
    }
    Ok(())
}
fn copy(
    source: &Path,
    destination: &Path,
    relative: &Path,
    entries: &mut Vec<serde_json::Value>,
    total: &mut u64,
) -> io::Result<()> {
    let meta = fs::symlink_metadata(source)?;
    if meta.file_type().is_symlink() {
        return Err(error("symlink in actual custody source"));
    }
    if source
        .extension()
        .and_then(|s| s.to_str())
        .is_some_and(|s| {
            matches!(
                s.to_ascii_lowercase().as_str(),
                "pem" | "key" | "crt" | "pfx" | "p12" | "csr"
            )
        })
        || source
            .file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|s| s.to_ascii_lowercase().contains("private"))
    {
        return Err(error("credential-like filename in selected DB root"));
    }
    if meta.is_dir() {
        fs::create_dir_all(destination)?;
        let mut children = fs::read_dir(source)?.collect::<io::Result<Vec<_>>>()?;
        children.sort_by_key(|entry| entry.file_name());
        for child in children {
            copy(
                &child.path(),
                &destination.join(child.file_name()),
                &relative.join(child.file_name()),
                entries,
                total,
            )?;
        }
        File::open(destination)?.sync_all()?;
        return Ok(());
    }
    if !meta.is_file()
        || meta.len() > MAX_FILE
        || entries.len() >= MAX_FILES
        || total.checked_add(meta.len()).is_none_or(|n| n > MAX_TOTAL)
    {
        return Err(error("bounded five-owner custody limit exceeded"));
    }
    let mut input = File::open(source)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut hash = Sha256::new();
    let mut bytes = 0;
    let mut buf = [0u8; 65536];
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        bytes += n as u64;
        if bytes > MAX_FILE || total.checked_add(bytes).is_none_or(|n| n > MAX_TOTAL) {
            return Err(error("actual source grew beyond custody bound"));
        }
        output.write_all(&buf[..n])?;
        hash.update(&buf[..n]);
    }
    output.sync_all()?;
    let hash = hex::encode(hash.finalize());
    let after = fs::metadata(source)?;
    if bytes != meta.len()
        || after.len() != meta.len()
        || after.modified()? != meta.modified()?
        || digest(source)? != hash
    {
        return Err(error("actual source changed during raw custody copy"));
    }
    *total += bytes;
    entries.push(serde_json::json!({"path":relative,"bytes":bytes,"sha256":hash,"source":source}));
    Ok(())
}
pub(super) fn capture(
    output: &Path,
    databases: &[Arc<RwLock<BicDb>>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
) -> io::Result<serde_json::Value> {
    if databases.len() != 5 || services.len() != 5 || metadata.len() != 5 {
        return Err(error("custody requires all five actual owners"));
    }
    // All acquisitions are nonwaiting. A refusal drops every partial guard;
    // no MetaMutex→DB wait, network or producer runs while these are held.
    // The owners stay alive, so their own workers hold these latches for
    // short moments; a refused cut is repeated as a whole, for a bounded
    // time, instead of failing on one such moment.
    let cut_started = std::time::Instant::now();
    let (metadata, _services, db) = loop {
        let cut = (|| {
            let metadata = metadata
                .iter()
                .map(|m| {
                    m.try_lock()
                        .ok_or_else(|| error("actual metadata busy at custody cut"))
                })
                .collect::<io::Result<Vec<_>>>()?;
            let services = services
                .iter()
                .map(|s| {
                    s.try_lock()
                        .ok_or_else(|| error("actual service busy at custody cut"))
                })
                .collect::<io::Result<Vec<_>>>()?;
            let db = databases
                .iter()
                .map(|d| {
                    d.try_write()
                        .ok_or_else(|| error("actual DB/callback busy at custody cut"))
                })
                .collect::<io::Result<Vec<_>>>()?;
            Ok::<_, io::Error>((metadata, services, db))
        })();
        match cut {
            Ok(guards) => break guards,
            Err(refused) if cut_started.elapsed() >= CUSTODY_CUT_WAIT => return Err(refused),
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(5)),
        }
    };
    let _journals = db
        .iter()
        .map(|d| {
            d.test_native_custody_journal()
                .map_err(|e| error(e.to_string()))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let destination = output.join("custody");
    fs::create_dir(&destination)?;
    let destination = destination.canonicalize()?;
    let sources = db
        .iter()
        .map(|d| d.data_path().canonicalize())
        .collect::<io::Result<Vec<_>>>()?;
    if sources.iter().any(|source| destination.starts_with(source)) {
        return Err(error("custody destination lies inside selected actual DB"));
    }
    let mut before = BTreeMap::new();
    let mut before_bytes = 0;
    for source in &sources {
        files(source, &mut before, &mut before_bytes)?;
    }
    let mut entries = Vec::new();
    let mut total = 0;
    for (i, source) in sources.iter().enumerate() {
        let name = PathBuf::from(format!("node{i}"));
        if let Err(primary) = copy(
            source,
            &destination.join(&name),
            &name,
            &mut entries,
            &mut total,
        ) {
            // Keep the first copy error even if recording partial evidence
            // also fails. Entries describe completed files only; an unfinished
            // destination file may exist and is not a verified snapshot.
            let partial = serde_json::json!({"status":"incomplete_copy",
                "error":primary.to_string(),"completed_files":entries,"completed_bytes":total,
                "unfinished_file_may_exist":true,"full_inventory_verified":false});
            if let Ok(bytes) = serde_json::to_vec_pretty(&partial) {
                let _ = crate::storage::write_atomic(
                    &output.join("custody-partial-files.json"),
                    &bytes,
                    true,
                );
            }
            return Err(primary);
        }
    }
    let mut after = BTreeMap::new();
    let mut after_bytes = 0;
    for source in &sources {
        files(source, &mut after, &mut after_bytes)?;
    }
    if before != after || entries.len() != before.len() {
        return Err(error(
            "actual file set or digest changed across custody cut",
        ));
    }
    let manifest = serde_json::json!({"version":1,"status":"observed_stable_with_live_actual_owners",
        "source":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),"package_version":env!("CARGO_PKG_VERSION"),
        "all_five_metadata_service_db_native_journal_guards_held":true,"metadata":metadata.iter().map(|m|m.status()).collect::<Vec<_>>(),
        "listeners_drained":false,"engine_owners_alive":true,"engine_reopened":false,
        "native_replay_or_checkpoint":false,"atomic_powerloss_snapshot_claimed":false,
        "only_actual_db_roots_including_owned_metadata":true,"tls_private_root_excluded":true,
        "files":entries,"total_bytes":total});
    crate::storage::write_atomic(
        &output.join("custody-manifest.json"),
        &serde_json::to_vec_pretty(&manifest)?,
        true,
    )
    .map_err(|e| error(e.to_string()))?;
    File::open(output)?.sync_all()?;
    Ok(manifest)
}
