//! `bicdb verify-store --read-only`: JSON report, exit status, findings
//! file, and byte-identical store files before and after the run.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use bicdb_core::{BicDb, DbConfig, StorageMode};
use sha2::{Digest, Sha256};

fn bicdb_bin() -> &'static str {
    env!("CARGO_BIN_EXE_bicdb")
}

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
}

fn hashes(dir: &Path) -> BTreeMap<PathBuf, (u64, String)> {
    let mut out = BTreeMap::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in std::fs::read_dir(&current).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                pending.push(path);
                continue;
            }
            let bytes = std::fs::read(&path).unwrap();
            out.insert(
                path.strip_prefix(dir).unwrap().to_path_buf(),
                (bytes.len() as u64, hex::encode(Sha256::digest(&bytes))),
            );
        }
    }
    out
}

fn run(args: &[&str]) -> (i32, serde_json::Value, String) {
    let output = Command::new(bicdb_bin()).args(args).output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let report = serde_json::from_slice(&output.stdout).unwrap_or(serde_json::Value::Null);
    (output.status.code().unwrap_or(-1), report, stderr)
}

fn sql(db: &mut BicDb, statement: &str) {
    bicdb_sql::SqlSession::new(db)
        .execute(statement)
        .unwrap_or_else(|error| panic!("{statement}: {error}"));
}

#[test]
fn verify_store_reads_a_churned_sql_database_without_changing_a_byte() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("db");
    {
        let mut db = BicDb::open_with_config(&path, config()).unwrap();
        sql(
            &mut db,
            "CREATE TABLE accounts(id INT PRIMARY KEY, owner TEXT, balance INT)",
        );
        sql(&mut db, "CREATE INDEX accounts_owner ON accounts(owner)");
        for batch in 0..20 {
            let values: Vec<String> = (0..200)
                .map(|row| {
                    let id = batch * 200 + row;
                    format!("({id}, 'owner-{}', {id})", id % 97)
                })
                .collect();
            sql(
                &mut db,
                &format!("INSERT INTO accounts VALUES {}", values.join(", ")),
            );
        }
        sql(
            &mut db,
            "UPDATE accounts SET balance = balance + 1 WHERE id % 3 = 0",
        );
        sql(
            &mut db,
            "UPDATE accounts SET owner = 'moved' WHERE id % 11 = 0",
        );
        sql(&mut db, "DELETE FROM accounts WHERE id % 7 = 0");
        sql(&mut db, "VACUUM");
        db.close().unwrap();
    }
    let before = hashes(&path);

    let (code, _, stderr) = run(&["verify-store", path.to_str().unwrap()]);
    assert_ne!(code, 0, "verify-store must refuse without --read-only");
    assert!(stderr.contains("--read-only"), "{stderr}");

    let findings = root.path().join("findings.jsonl");
    let (code, report, stderr) = run(&[
        "verify-store",
        "--read-only",
        path.to_str().unwrap(),
        "--findings-out",
        findings.to_str().unwrap(),
    ]);
    assert_eq!(hashes(&path), before, "verify-store changed the database");
    assert!(report.is_object(), "no JSON report: {stderr}");
    eprintln!(
        "CLI-SQL exit={code} verdict={} classes={}",
        report["verdict"],
        serde_json::to_string(
            &report["classes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|class| (class["class"].clone(), class["count"].clone()))
                .collect::<Vec<_>>()
        )
        .unwrap()
    );
    // Deleted rows leave dangling index entries after VACUUM (needs a
    // cross-check); nothing may be inconsistent.
    assert!(code == 0 || code == 3, "exit {code}: {report:#}");
    assert_ne!(report["verdict"], "inconsistent", "{report:#}");
    assert_eq!(report["complete"], true);
    assert_eq!(report["read_only"]["recovery_ran"], false);
    assert!(report["counters"]["visible_rows"].as_u64().unwrap() > 0);
    let (limited_code, limited, _) = run(&[
        "verify-store",
        "--read-only",
        path.to_str().unwrap(),
        "--max-seconds",
        "0",
    ]);
    assert_eq!(
        limited_code, 4,
        "a setup limit must be an incomplete report"
    );
    assert_eq!(limited["complete"], false);
    assert_eq!(limited["stop_reason"], "time limit");
    // Every finding line names the record layer's collection when the key
    // has that layout.
    let lines = std::fs::read_to_string(&findings).unwrap();
    for line in lines.lines() {
        let finding: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(finding["class"].is_string());
        if finding["key"].is_string() {
            assert!(finding["collection"].is_string(), "{line}");
        }
    }

    // A findings file inside the store is refused, and so is an existing one.
    let (code, _, stderr) = run(&[
        "verify-store",
        "--read-only",
        path.to_str().unwrap(),
        "--findings-out",
        path.join("paged/out.jsonl").to_str().unwrap(),
    ]);
    assert_ne!(code, 0);
    assert!(stderr.contains("outside the store"), "{stderr}");
    let (code, _, stderr) = run(&[
        "verify-store",
        "--read-only",
        path.to_str().unwrap(),
        "--findings-out",
        path.join("out.jsonl").to_str().unwrap(),
    ]);
    assert_ne!(code, 0);
    assert!(stderr.contains("outside the store"), "{stderr}");
    let (code, _, _) = run(&[
        "verify-store",
        "--read-only",
        path.to_str().unwrap(),
        "--findings-out",
        findings.to_str().unwrap(),
    ]);
    assert_ne!(code, 0);
    assert_eq!(hashes(&path), before);
}

#[test]
fn verify_store_exit_status_reports_a_stale_free_space_record() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("store");
    let options = bicdb_page::PagedStoreOptions::default()
        .with_page_size(4096)
        .with_buffer_pool_bytes(4 << 20)
        .with_fsync(false);
    {
        let (store, _) = bicdb_page::PagedStore::open(&dir, options.clone()).unwrap();
        let xid = store.begin();
        for index in 0..300 {
            store
                .put(xid, format!("gone-{index:04}").as_bytes(), &[b'g'; 100])
                .unwrap();
        }
        store.commit(xid).unwrap();
        let xid = store.begin();
        for index in 0..300 {
            store
                .put(xid, format!("kept-{index:04}").as_bytes(), b"kept")
                .unwrap();
        }
        store.commit(xid).unwrap();
        let xid = store.begin();
        for index in 0..300 {
            store
                .delete(xid, format!("gone-{index:04}").as_bytes())
                .unwrap();
        }
        store.commit(xid).unwrap();
        while store.vacuum(1 << 20).unwrap().versions_reclaimed > 0 {}
        store.sweep_dead_index_entries(usize::MAX).unwrap();
        // Advertise a non-heap page, as the delete path's bookkeeping drift does.
        let page_size = 4096u64;
        store.checkpoint().unwrap();
        let pages = std::fs::metadata(dir.join("store.pages")).unwrap().len() / page_size;
        let freed = (1..pages)
            .find(|page| {
                use std::os::unix::fs::FileExt;
                let file = std::fs::File::open(dir.join("store.pages")).unwrap();
                let mut header = [0u8; 8];
                file.read_exact_at(&mut header, page * page_size).unwrap();
                // A freed page if vacuum freed one, else any B-tree page:
                // either is a page an insert must never write to.
                header[6] == 6 || header[6] == 5
            })
            .expect("a non-heap page");
        store.catalog().update_free_space(freed, 0, 4000).unwrap();
        store.checkpoint().unwrap();
    }
    let before = hashes(&dir);
    let (code, report, stderr) = run(&["verify-store", "--read-only", dir.to_str().unwrap()]);
    assert_eq!(hashes(&dir), before);
    assert_eq!(code, 3, "{stderr}\n{report:#}");
    assert_eq!(report["verdict"], "at_risk");
    assert!(report["classes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|class| class["class"] == "stale_free_space_record"));
}
