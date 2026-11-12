//! Optional diagnostic custody only. This module creates no engine proof and
//! never opens a DB/MStore, changes a fault, or catches a safety assertion.
use super::*;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    OnceLock,
};

const MAX_FILES: usize = 8192;
const MAX_NODES: usize = 16384;
const MAX_DEPTH: usize = 32;
const MAX_REPORTS: usize = 256;
const MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
struct Evidence {
    root: PathBuf,
    source_sha: Option<String>,
    sequence: AtomicUsize,
}
fn source_sha() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["-C", env!("CARGO_MANIFEST_DIR"), "rev-parse", "HEAD"])
        .output()
        .ok()?;
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (output.status.success() && sha.len() == 40 && sha.bytes().all(|b| b.is_ascii_hexdigit()))
        .then_some(sha)
}
fn evidence() -> Option<&'static Evidence> {
    static EVIDENCE: OnceLock<Option<Evidence>> = OnceLock::new();
    EVIDENCE
        .get_or_init(|| {
            let requested = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT")?;
            let result = (|| -> std::io::Result<Evidence> {
                let base = PathBuf::from(requested);
                std::fs::create_dir_all(&base)?;
                let root = base.join(format!("native-read-missing-catalogue-{}", Uuid::now_v7()));
                std::fs::create_dir(&root)?;
                std::fs::create_dir(root.join("reports"))?;
                Ok(Evidence {
                    root,
                    source_sha: source_sha(),
                    sequence: AtomicUsize::new(0),
                })
            })();
            match result {
                Ok(evidence) => {
                    eprintln!(
                        "NATIVE_MISSING_CATALOGUE_EVIDENCE {}",
                        evidence.root.display()
                    );
                    Some(evidence)
                }
                Err(error) => {
                    eprintln!("NATIVE_MISSING_CATALOGUE_EVIDENCE_ERROR {error}");
                    None
                }
            }
        })
        .as_ref()
}
fn write_json(path: &Path, value: &Value) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(&serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?)?;
    file.sync_all()?;
    std::fs::File::open(path.parent().unwrap())?.sync_all()
}
fn public_report(report: &Value) -> Value {
    let mut report = report.clone();
    if let Some(object) = report.as_object_mut() {
        object.remove("original_private_metadata_tls");
        object.insert("private_tls_directory_excluded".into(), json!(true));
    }
    report
}
pub(super) fn mirror_report(report: &Value) {
    let Some(evidence) = evidence() else {
        return;
    };
    let sequence = evidence.sequence.fetch_add(1, Ordering::SeqCst);
    if sequence >= MAX_REPORTS {
        if sequence == MAX_REPORTS {
            eprintln!("NATIVE_MISSING_CATALOGUE_REPORT_LIMIT_REACHED");
        }
        return;
    }
    let value = json!({"evidence_version":1,"source_sha":evidence.source_sha,"source_verified":evidence.source_sha.is_some(),"report":public_report(report)});
    if let Err(error) = write_json(
        &evidence
            .root
            .join("reports")
            .join(format!("{sequence:05}.json")),
        &value,
    ) {
        // Diagnostic IO cannot alter the engine result or turn a failure green.
        eprintln!("NATIVE_MISSING_CATALOGUE_REPORT_MIRROR_ERROR {error}");
    }
}
#[derive(serde::Serialize)]
struct FileRecord {
    relative_path: String,
    bytes: u64,
    sha256: String,
}
fn hash_file(path: &Path, expected: u64) -> std::io::Result<String> {
    let mut input = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut bytes = 0u64;
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes = bytes
            .checked_add(count as u64)
            .ok_or_else(|| std::io::Error::other("custody byte count overflow"))?;
        if bytes > expected {
            return Err(std::io::Error::other("stopped custody file grew"));
        }
        hash.update(&buffer[..count]);
    }
    if bytes != expected {
        return Err(std::io::Error::other("stopped custody file size changed"));
    }
    Ok(hex::encode(hash.finalize()))
}
fn inventory(
    source: &Path,
    relative: &Path,
    files: &mut Vec<FileRecord>,
    directories: &mut Vec<String>,
    total: &mut u64,
    nodes: &mut usize,
    depth: usize,
) -> std::io::Result<()> {
    *nodes += 1;
    if *nodes > MAX_NODES || depth > MAX_DEPTH {
        return Err(std::io::Error::other("bounded custody tree limit exceeded"));
    }
    let metadata = std::fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::other("custody refuses source symlinks"));
    }
    if metadata.is_dir() {
        directories.push(
            relative
                .to_str()
                .ok_or_else(|| std::io::Error::other("custody non-UTF8 directory"))?
                .into(),
        );
        let mut entries = std::fs::read_dir(source)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            inventory(
                &entry.path(),
                &relative.join(entry.file_name()),
                files,
                directories,
                total,
                nodes,
                depth + 1,
            )?;
        }
    } else if metadata.is_file() {
        *total = total
            .checked_add(metadata.len())
            .ok_or_else(|| std::io::Error::other("custody total overflow"))?;
        if files.len() >= MAX_FILES || metadata.len() > MAX_FILE_BYTES || *total > MAX_TOTAL_BYTES {
            return Err(std::io::Error::other(
                "bounded custody file/byte limit exceeded",
            ));
        }
        files.push(FileRecord {
            relative_path: relative
                .to_str()
                .ok_or_else(|| std::io::Error::other("custody non-UTF8 path"))?
                .into(),
            bytes: metadata.len(),
            sha256: hash_file(source, metadata.len())?,
        });
    } else {
        return Err(std::io::Error::other(
            "custody refuses special source files",
        ));
    }
    Ok(())
}
/// Called ONLY after actual DB AND directory Weak proofs expired. Only these
/// six explicit trees are copied; the enclosing TLS key/certificate root and
/// live/intact/missing case images are never traversed.
pub(super) fn retain_stopped_original(data: &Path, metadata_and_tls: &Path, report: &Value) {
    let Some(evidence) = evidence() else {
        return;
    };
    let result = (|| -> std::io::Result<()> {
        let stopped = evidence.root.join("stopped-original");
        std::fs::create_dir(&stopped)?;
        let mut files = Vec::new();
        let mut total = 0u64;
        let mut directories = Vec::new();
        let mut nodes = 0usize;
        let mut roots = Vec::new();
        for name in ["n0", "n1", "n2"] {
            for (kind, source, relative) in [
                (
                    "native_database",
                    data.join(name),
                    PathBuf::from("data").join(name),
                ),
                (
                    "owned_metadata",
                    metadata_and_tls.join(format!("strong-meta-{name}")),
                    PathBuf::from("metadata").join(format!("strong-meta-{name}")),
                ),
            ] {
                if !std::fs::symlink_metadata(&source)?.is_dir() {
                    return Err(std::io::Error::other(
                        "explicit stopped custody root is not a directory",
                    ));
                }
                inventory(
                    &source,
                    &relative,
                    &mut files,
                    &mut directories,
                    &mut total,
                    &mut nodes,
                    0,
                )?;
                roots.push((kind, source, relative));
            }
        }
        for directory in &directories {
            std::fs::create_dir_all(stopped.join(directory))?;
        }
        for record in &files {
            let root = roots
                .iter()
                .find(|(_, _, relative)| Path::new(&record.relative_path).starts_with(relative))
                .ok_or_else(|| std::io::Error::other("custody file escaped explicit roots"))?;
            let suffix = Path::new(&record.relative_path)
                .strip_prefix(&root.2)
                .map_err(std::io::Error::other)?;
            let source = root.1.join(suffix);
            let destination = stopped.join(&record.relative_path);
            std::fs::create_dir_all(destination.parent().unwrap())?;
            // The source inventory was captured only after owner drain. Recheck
            // exact source hash/size and actual copied hash before claiming it.
            if hash_file(&source, record.bytes)? != record.sha256 {
                return Err(std::io::Error::other("stopped custody source changed"));
            }
            let input = std::fs::File::open(&source)?;
            let mut output = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&destination)?;
            let copied = std::io::copy(&mut input.take(record.bytes + 1), &mut output)?;
            if copied != record.bytes {
                return Err(std::io::Error::other("stopped custody copied size differs"));
            }
            output.sync_all()?;
            if hash_file(&destination, record.bytes)? != record.sha256 {
                return Err(std::io::Error::other("stopped custody copied hash differs"));
            }
        }
        for directory in directories.iter().rev() {
            std::fs::File::open(stopped.join(directory))?.sync_all()?;
        }
        for directory in ["data", "metadata"] {
            std::fs::File::open(stopped.join(directory))?.sync_all()?;
        }
        let manifest = json!({"evidence_version":1,"source_sha":evidence.source_sha,"source_verified":evidence.source_sha.is_some(),
            "state":"FULLY_STOPPED_ORIGINAL_ONLY","private_tls_excluded":true,"live_case_images_copied":false,
            "bounds":{"max_files":MAX_FILES,"max_tree_nodes":MAX_NODES,"max_depth":MAX_DEPTH,"max_file_bytes":MAX_FILE_BYTES,"max_total_bytes":MAX_TOTAL_BYTES},
            "file_count":files.len(),"total_bytes":total,"files":files,"directories":directories,"original_report":public_report(report)});
        let manifest_path = stopped.join("manifest.json");
        write_json(&manifest_path, &manifest)?;
        let manifest_sha = hash_file(&manifest_path, std::fs::metadata(&manifest_path)?.len())?;
        let mut digest = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(stopped.join("manifest.sha256"))?;
        writeln!(digest, "{manifest_sha}  manifest.json")?;
        digest.sync_all()?;
        std::fs::File::open(&stopped)?.sync_all()?;
        std::fs::File::open(&evidence.root)?.sync_all()?;
        eprintln!(
            "NATIVE_MISSING_CATALOGUE_STOPPED_CUSTODY {} files={} bytes={total} manifest_sha256={manifest_sha}",
            stopped.display(),
            files.len()
        );
        Ok(())
    })();
    if let Err(error) = result {
        eprintln!("NATIVE_MISSING_CATALOGUE_STOPPED_CUSTODY_ERROR {error}");
        let _ = write_json(
            &evidence.root.join("custody-error.json"),
            &json!({"source_sha":evidence.source_sha,"error":error.to_string(),"partial_copy_is_not_verified_custody":true}),
        );
    }
}
