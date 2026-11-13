//! Genuine process loss at actual AppliedKnown; normal open must preserve debt.
use super::*;
use std::io::Write;
use std::os::unix::process::ExitStatusExt;
mod stopped_custody;
struct ChildCustody(std::process::Child);
impl Drop for ChildCustody {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
fn business_bytes(root: &std::path::Path) -> std::collections::BTreeMap<String, Option<Vec<u8>>> {
    [
        crate::DEFAULT_TRANSACTION_LOG,
        "sync.log",
        "events/events.seg",
        "incarnations.log",
    ]
    .into_iter()
    .map(|name| {
        let path = root.join(name);
        let bytes = match std::fs::read(path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => panic!("read exact business file {name}: {error}"),
        };
        (name.to_owned(), bytes)
    })
    .collect()
}
#[test]
fn native_registered_fifth_row_cold_sigkill_preserves_pending_before_generic_open() {
    if std::env::var_os(COLD_CHILD_MARKER).is_some() {
        run_registered_image_baseline_with_source(
            Case::Required,
            Some(SourceCase::TargetImageRowsPostWalCanceled),
        );
        panic!("actual post-applied seam was not reached");
    }
    let temporary = tempfile::tempdir().unwrap();
    let output = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| temporary.path().to_path_buf())
        .join(format!(
            "native-registered-row-cold-preservation-{}",
            Uuid::now_v7()
        ));
    std::fs::create_dir_all(&output).unwrap();
    let marker = output.join("actual-applied-known-marker.json");
    let module = module_path!().split_once("::").unwrap().1;
    let exact = format!(
        "{module}::native_registered_fifth_row_cold_sigkill_preserves_pending_before_generic_open"
    );
    let stdout = std::fs::File::create(output.join("child.stdout.log")).unwrap();
    let stderr = std::fs::File::create(output.join("child.stderr.log")).unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &exact, "--test-threads=1", "--nocapture"])
        .env(COLD_CHILD_MARKER, &marker)
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .unwrap();
    let mut child = ChildCustody(child);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if marker.try_exists().unwrap() {
            break;
        }
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited before actual applied-known marker; see retained logs"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "actual cold seam not reached within separate process bound"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let actual_marker: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&marker).unwrap()).unwrap();
    assert_eq!(
        actual_marker["phase"],
        "actual_applied_known_before_protected_progress"
    );
    assert_eq!(
        actual_marker["child_pid"].as_u64(),
        Some(child.0.id() as u64)
    );
    child.0.kill().unwrap(); // SIGKILL on Unix; no Rust Drop/close in child.
    let terminal = child.0.wait().unwrap();
    assert_eq!(terminal.signal(), Some(9));
    let pre: serde_json::Value =
        serde_json::from_slice(&std::fs::read(marker.with_extension("pre.json")).unwrap()).unwrap();
    let root = PathBuf::from(pre["target_database_path"].as_str().unwrap());
    let fixture_root = PathBuf::from(pre["owned_fixture_root"].as_str().unwrap());
    // All five node services lived in this one child process. They are now
    // stopped, but custody here copies ONLY the target, before its first open.
    let stopped_target_custody = stopped_custody::capture(&root, &fixture_root, &output);
    let before = business_bytes(&root);
    let wal = before[crate::DEFAULT_TRANSACTION_LOG].as_ref().unwrap();
    assert!(!wal.is_empty(), "real original row logical WAL required");
    let mut raw = std::fs::File::create(output.join("retained-original-logical-wal.bin")).unwrap();
    raw.write_all(wal).unwrap();
    raw.sync_all().unwrap();
    drop(raw);
    // First reopen ONLY the actual native page store to retain restrictive DATA;
    // it does not run BicDb logical recovery, incarnation append or outboxes.
    let config = db_config();
    let paged = crate::paged_collection::PagedRecords::open(
        root.join("paged"),
        crate::paged_collection::PagedRecordsOptions {
            page_size: config.paged_page_size,
            buffer_pool_bytes: config.paged_buffer_pool_bytes,
            read_ahead_queue_pages: config.paged_read_ahead_queue_pages,
            fsync: config.fsync,
            wal_max_bytes: config.paged_wal_max_bytes,
            wal_segment_bytes: config.paged_wal_segment_bytes,
            extent_bytes: config.paged_extent_bytes,
            accept_legacy_meta: config.paged_accept_legacy_meta,
        },
    )
    .unwrap();
    let pending = crate::db::registered_row_retention::inspect_for_test(&root, &paged).unwrap();
    assert_eq!(pending["pending"].as_array().unwrap().len(), 1);
    let intent = &pending["pending"][0]["intent"];
    assert!(intent["logical_txid"].as_u64().unwrap() > 0);
    assert_eq!(intent["mutations_sha256"].as_str().unwrap().len(), 64);
    drop(paged);
    let error = match BicDb::open_with_config(&root, db_config()) {
        Ok(_) => panic!("normal open must not consume original pending row evidence"),
        Err(error) => error,
    };
    let after = business_bytes(&root);
    evidence(&output.join("registered-row-cold-preservation.json"),&serde_json::json!({
        "actual_marker":actual_marker,"signal":terminal.signal(),"pre":pre,"actual_pending":pending,
        "stopped_target_custody":stopped_target_custody,
        "all_five_fixture_nodes_died_in_same_process":true,
        "custody_scope":"stopped target only, copied before first native recovery open",
        "independent_host_or_other_four_node_custody_claim":false,
        "exact_open_error":error.to_string(),"logical_wal_bytes":wal.len(),
        "logical_wal_sha256":hex::encode(Sha256::digest(wal)),
        "business_file_comparison":before.iter().map(|(name,bytes)|serde_json::json!({
            "path":name,"present":bytes.is_some(),"bytes":bytes.as_ref().map(Vec::len),
            "sha256":bytes.as_ref().map(|b|hex::encode(Sha256::digest(b))),
            "unchanged":after.get(name)==Some(bytes)})).collect::<Vec<_>>(),
        "cold_repair_implemented":false,"serving_or_ha_qualification":false
    })).unwrap();
    assert!(
        error
            .to_string()
            .contains("cold repair required before logical replay/cut"),
        "{error}"
    );
    assert_eq!(
        after, before,
        "fresh normal open must refuse before WAL/scanner/incarnation/outbox changes"
    );
}
