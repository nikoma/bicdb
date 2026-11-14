//! Byte-verified custody of this fixture's stopped target before any reopen.
use super::*;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path};
const MAX_FILES: usize = 4096;
const MAX_DIRECTORIES: usize = 1024;
const MAX_FILE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const MAX_DEPTH: usize = 16;
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
struct FileFact {
    kind: &'static str,
    bytes: u64,
    sha256: Option<String>,
}
fn regular_file(path: &Path) -> std::fs::File {
    let before = std::fs::symlink_metadata(path).unwrap();
    assert!(
        before.is_file() && !before.file_type().is_symlink(),
        "custody rejects links/non-regular files: {}",
        path.display()
    );
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .unwrap();
    let actual = file.metadata().unwrap();
    assert!(actual.is_file());
    assert_eq!(
        (actual.dev(), actual.ino(), actual.len()),
        (before.dev(), before.ino(), before.len()),
        "source changed during custody open"
    );
    file
}
fn file_fact(path: &Path) -> FileFact {
    let mut file = regular_file(path);
    let expected = file.metadata().unwrap().len();
    assert!(expected <= MAX_FILE_BYTES, "custody per-file bound");
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let got = file.read(&mut buffer).unwrap();
        if got == 0 {
            break;
        }
        bytes = bytes.checked_add(got as u64).unwrap();
        assert!(bytes <= MAX_FILE_BYTES);
        hash.update(&buffer[..got]);
    }
    assert_eq!(bytes, expected, "source file changed while hashing");
    FileFact {
        kind: "file",
        bytes,
        sha256: Some(hex::encode(hash.finalize())),
    }
}
fn inventory(root: &Path) -> BTreeMap<String, FileFact> {
    fn walk(
        root: &Path,
        relative: &Path,
        facts: &mut BTreeMap<String, FileFact>,
        files: &mut usize,
        dirs: &mut usize,
        total: &mut u64,
    ) {
        assert!(relative
            .components()
            .all(|c| matches!(c, Component::Normal(_))));
        assert!(
            relative.components().count() <= MAX_DEPTH,
            "custody depth bound"
        );
        let path = root.join(relative);
        let meta = std::fs::symlink_metadata(&path).unwrap();
        assert!(
            !meta.file_type().is_symlink(),
            "custody rejects symlink {}",
            path.display()
        );
        assert_eq!(
            path.canonicalize().unwrap(),
            path,
            "custody path escaped canonical owned root"
        );
        if meta.is_dir() {
            *dirs += 1;
            assert!(*dirs <= MAX_DIRECTORIES, "custody directory bound");
            if !relative.as_os_str().is_empty() {
                facts.insert(
                    relative.to_str().unwrap().into(),
                    FileFact {
                        kind: "directory",
                        bytes: 0,
                        sha256: None,
                    },
                );
            }
            for entry in std::fs::read_dir(&path).unwrap() {
                let entry = entry.unwrap();
                walk(
                    root,
                    &relative.join(entry.file_name()),
                    facts,
                    files,
                    dirs,
                    total,
                );
            }
        } else {
            assert!(meta.is_file(), "custody rejects sockets/devices/non-files");
            *files += 1;
            assert!(*files <= MAX_FILES, "custody file bound");
            let fact = file_fact(&path);
            *total = total.checked_add(fact.bytes).unwrap();
            assert!(*total <= MAX_TOTAL_BYTES, "custody total byte bound");
            assert!(facts
                .insert(relative.to_str().unwrap().into(), fact)
                .is_none());
        }
    }
    let mut facts = BTreeMap::new();
    let (mut files, mut dirs, mut total) = (0, 0, 0);
    walk(
        root,
        Path::new(""),
        &mut facts,
        &mut files,
        &mut dirs,
        &mut total,
    );
    facts
}
fn private_directory(path: &Path) {
    std::fs::create_dir(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}
pub(super) fn capture(source: &Path, fixture_root: &Path, output: &Path) -> serde_json::Value {
    assert!(!std::fs::symlink_metadata(source)
        .unwrap()
        .file_type()
        .is_symlink());
    let source = source.canonicalize().unwrap();
    let fixture = fixture_root.canonicalize().unwrap();
    assert_eq!(
        source,
        fixture.join("actual-registered-fifth-image-target"),
        "custody source must be this exact task target, never an arbitrary directory"
    );
    let output = output.canonicalize().unwrap();
    assert!(
        !output.starts_with(&source),
        "custody destination cannot be inside source"
    );
    let original = inventory(&source);
    let custody = output.join("custody");
    private_directory(&custody);
    let destination = custody.join("stopped-target-before-first-open");
    private_directory(&destination);
    for (relative, fact) in &original {
        let target = destination.join(relative);
        if fact.kind == "directory" {
            private_directory(&target);
            continue;
        }
        let mut input = regular_file(&source.join(relative));
        let mut writer = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&target)
            .unwrap();
        let mut remaining = fact.bytes;
        let mut buffer = [0u8; 64 * 1024];
        while remaining > 0 {
            let maximum = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
            let got = input.read(&mut buffer[..maximum]).unwrap();
            assert!(got > 0, "source shrank during custody copy");
            writer.write_all(&buffer[..got]).unwrap();
            remaining -= got as u64;
        }
        assert_eq!(
            input.read(&mut buffer[..1]).unwrap(),
            0,
            "source grew during custody copy"
        );
        writer.sync_all().unwrap();
    }
    // Durable private file contents and directory entries before recovery can
    // touch the original. Never open/replay this retained copy with an engine.
    for (relative, fact) in original.iter().rev() {
        if fact.kind == "directory" {
            std::fs::File::open(destination.join(relative))
                .unwrap()
                .sync_all()
                .unwrap();
        }
    }
    std::fs::File::open(&destination)
        .unwrap()
        .sync_all()
        .unwrap();
    std::fs::File::open(&custody).unwrap().sync_all().unwrap();
    let original_after = inventory(&source);
    let copied = inventory(&destination);
    let report = serde_json::json!({
        "source":source,"private_destination":destination,"inventory":original,
        "original_inventory_after_copy":original_after,"copied_inventory":copied,
        "original_inventory_unchanged":original==original_after,"copy_inventory_identical":original==copied,
        "bounds":{"files":MAX_FILES,"directories":MAX_DIRECTORIES,"depth":MAX_DEPTH,
            "file_bytes":MAX_FILE_BYTES,"total_bytes":MAX_TOTAL_BYTES},
        "source_process_terminated_by_sigkill_before_capture":true,
        "all_five_fixture_nodes_were_in_that_process":true,
        "copied_scope":"target only; all regular files and empty directories",
        "copied_before_any_paged_or_bicdb_reopen":true,"retained_copy_never_engine_opened":true,
        "qualification":"stopped-target file custody, not cold replay or independent-host HA"
    });
    evidence(
        &output.join("stopped-target-custody-inventory.json"),
        &report,
    )
    .unwrap();
    assert_eq!(
        original, original_after,
        "stopped source changed while copying"
    );
    assert_eq!(original, copied, "stopped-target custody copy differs");
    report
}
