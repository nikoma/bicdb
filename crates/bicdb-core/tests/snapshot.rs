//! Binary startup snapshot ("true checkpoint") correctness matrix.
//!
//! The snapshot is a derived cache written at `compact()` and read at open. These
//! tests pin the contract the design rests on: it accelerates open when valid and
//! falls back to the JSON segment path on EVERY failure mode, never losing data.
//!
//! Scenarios (as requested):
//!   1. normal open (snapshot disabled)        -> baseline data integrity
//!   2. snapshot open                          -> files written, data loads from it
//!   3. corrupt snapshot fallback              -> torn records.snap -> segments
//!   4. missing snapshot fallback              -> no records.snap   -> segments
//!   5. version mismatch fallback              -> bad format_version -> segments
//!   6. WAL tail replay                        -> snapshot + post-compact commits
//!   7. crash during snapshot write            -> records.snap but no manifest

use std::fs;
use std::path::{Path, PathBuf};

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;

/// Pinned to `embedded_memory`: the binary snapshot is an accelerator for the
/// segment open path, and in paged mode segments carry no rows — open
/// bootstraps from the page store instead, so there is nothing here to test.
fn snapshot_on() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_snapshot(true)
        .with_storage_mode(StorageMode::EmbeddedMemory)
}

fn snapshot_off() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_snapshot(false)
        .with_storage_mode(StorageMode::EmbeddedMemory)
}

fn manifest_path(root: &Path) -> PathBuf {
    root.join("snapshot").join("manifest.json")
}

fn records_path(root: &Path) -> PathBuf {
    root.join("snapshot").join("records.snap")
}

/// Insert `count` records (vector + metadata) into `collection`.
fn seed(db: &mut BicDb, collection: &str, count: usize) {
    db.create_collection(collection).unwrap();
    for i in 0..count {
        let record = Record::new(format!("id-{i}"))
            .with_vector(vec![i as f32, (i * 2) as f32, (i * 3) as f32])
            .with_metadata(json!({"n": i, "label": format!("row-{i}")}))
            .with_timestamp(1_700_000_000 + i as i64);
        db.insert(collection, record).unwrap();
    }
}

/// Assert every seeded record is present and intact.
fn assert_all_present(db: &BicDb, collection: &str, count: usize) {
    for i in 0..count {
        let got = db
            .get(collection, &format!("id-{i}"))
            .unwrap()
            .unwrap_or_else(|| panic!("record id-{i} missing"));
        assert_eq!(got.metadata, json!({"n": i, "label": format!("row-{i}")}));
        assert_eq!(
            got.vector,
            Some(vec![i as f32, (i * 2) as f32, (i * 3) as f32])
        );
    }
}

#[test]
fn normal_open_with_snapshot_disabled_is_intact() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), snapshot_off()).unwrap();
        seed(&mut db, "c", 50);
        db.compact().unwrap();
        db.close().unwrap();
    }
    // Disabled => no snapshot files are produced at all.
    assert!(!manifest_path(temp.path()).exists());

    let db = BicDb::open_with_config(temp.path(), snapshot_off()).unwrap();
    assert_all_present(&db, "c", 50);
}

#[test]
fn snapshot_open_loads_from_binary_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
        seed(&mut db, "c", 50);
        db.compact().unwrap();
        db.close().unwrap();
    }
    // Compaction published the snapshot.
    assert!(manifest_path(temp.path()).exists(), "manifest written");
    assert!(records_path(temp.path()).exists(), "records.snap written");

    // Re-open with snapshot on: the heap is rebuilt from the binary snapshot.
    let db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
    assert_all_present(&db, "c", 50);
}

#[test]
fn snapshot_is_used_segment_is_not_read() {
    // Positive proof the binary snapshot is actually the read source: clobber the
    // segment in place (KEEPING its byte length, so the staleness check still
    // matches) with garbage. If the open fell back to the segment it would read
    // garbage and lose the records; succeeding proves the segment was never read.
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
        seed(&mut db, "c", 50);
        db.compact().unwrap();
        db.close().unwrap();
    }
    let segment = temp.path().join("segments").join("c.seg");
    let len = fs::metadata(&segment).unwrap().len();
    assert!(len > 0);
    fs::write(&segment, vec![0xFFu8; len as usize]).unwrap();

    let db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
    assert_all_present(&db, "c", 50); // only possible from the snapshot
}

#[test]
fn corrupt_snapshot_falls_back_to_segments() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
        seed(&mut db, "c", 80);
        db.compact().unwrap();
        db.close().unwrap();
    }
    // Truncate records.snap to half -> torn frames -> CRC/count mismatch.
    let records = records_path(temp.path());
    let len = fs::metadata(&records).unwrap().len();
    let file = fs::OpenOptions::new().write(true).open(&records).unwrap();
    file.set_len(len / 2).unwrap();
    drop(file);

    // Falls back to the JSON segments; no data lost.
    let db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
    assert_all_present(&db, "c", 80);
}

#[test]
fn missing_snapshot_falls_back_to_segments() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
        seed(&mut db, "c", 40);
        db.compact().unwrap();
        db.close().unwrap();
    }
    fs::remove_file(records_path(temp.path())).unwrap();

    let db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
    assert_all_present(&db, "c", 40);
}

#[test]
fn version_mismatch_falls_back_to_segments() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
        seed(&mut db, "c", 40);
        db.compact().unwrap();
        db.close().unwrap();
    }
    // Rewrite the manifest with a future format version the reader rejects.
    let manifest = manifest_path(temp.path());
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    value["format_version"] = json!(99_999);
    fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();

    let db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
    assert_all_present(&db, "c", 40);
}

#[test]
fn tail_merge_applies_post_snapshot_writes() {
    // Commits eagerly append materialized records to the segment, growing it past
    // the size recorded in the snapshot. On open the collection loads the snapshot's
    // clean prefix and decodes only the appended tail, applying it last-write-wins —
    // so it sees every post-snapshot write: brand-new records AND updates to records
    // that were already in the snapshot.
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
        seed(&mut db, "c", 30);
        db.compact().unwrap(); // snapshot captures the first 30

        // A brand-new record committed after the snapshot:
        db.insert(
            "c",
            Record::new("id-new")
                .with_metadata(json!({"fresh": true}))
                .with_timestamp(1),
        )
        .unwrap();
        // And an UPDATE to a record that IS in the snapshot (different value):
        db.insert(
            "c",
            Record::new("id-5").with_metadata(json!({"n": 5, "label": "updated"})),
        )
        .unwrap();
        db.close().unwrap();
    }
    // Snapshot is still present (only rewrites/compactions invalidate it).
    assert!(manifest_path(temp.path()).exists());

    let db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
    // Untouched snapshot-prefix record:
    let got = db.get("c", "id-0").unwrap().unwrap();
    assert_eq!(got.metadata, json!({"n": 0, "label": "row-0"}));
    // New tail record present:
    let fresh = db.get("c", "id-new").unwrap().unwrap();
    assert_eq!(fresh.metadata, json!({"fresh": true}));
    // Tail update wins over the snapshot value:
    let updated = db.get("c", "id-5").unwrap().unwrap();
    assert_eq!(updated.metadata, json!({"n": 5, "label": "updated"}));
}

#[test]
fn tail_merge_applies_post_snapshot_delete() {
    // A record present in the snapshot but DELETED after the snapshot must be gone on
    // reopen: the tail's Delete frame removes it from the snapshot-seeded prefix map.
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
        seed(&mut db, "c", 20);
        db.compact().unwrap();
        assert!(db.delete("c", "id-7").unwrap());
        db.close().unwrap();
    }
    let db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
    assert!(db.get("c", "id-7").unwrap().is_none(), "deleted in tail");
    assert!(db.get("c", "id-6").unwrap().is_some());
    assert!(db.get("c", "id-8").unwrap().is_some());
}

#[test]
fn wal_tail_reads_prefix_from_snapshot_not_segment() {
    // Positive proof post-snapshot recovery reads the PREFIX from the snapshot, not
    // the segment: after a post-snapshot WAL commit, clobber the segment's prefix
    // bytes (the region the snapshot covers). A full segment decode would choke on
    // the corrupt prefix; snapshot open ignores those bytes and still recovers every
    // prefix record from the snapshot plus the WAL tail.
    let temp = tempfile::tempdir().unwrap();
    let prefix_len = {
        let mut db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
        seed(&mut db, "c", 40);
        db.compact().unwrap();
        let len: u64 = serde_json::from_slice::<serde_json::Value>(
            &fs::read(manifest_path(temp.path())).unwrap(),
        )
        .unwrap()["collections"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == "c")
            .unwrap()["segment_byte_len"]
            .as_u64()
            .unwrap();
        // Commit a tail record AFTER capturing the prefix length.
        db.insert(
            "c",
            Record::new("id-tail").with_metadata(json!({"tail": true})),
        )
        .unwrap();
        db.close().unwrap();
        len
    };

    // Clobber the prefix in place (bytes [0, prefix_len)); the tail is in the WAL.
    let segment = temp.path().join("segments").join("c.seg");
    let total = fs::metadata(&segment).unwrap().len();
    assert_eq!(total, prefix_len, "post-snapshot tail should be WAL-only");
    let mut bytes = fs::read(&segment).unwrap();
    for b in bytes.iter_mut().take(prefix_len as usize) {
        *b = 0xFF;
    }
    fs::write(&segment, &bytes).unwrap();

    let db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
    assert_all_present(&db, "c", 40); // prefix recovered from the snapshot
    let tail = db.get("c", "id-tail").unwrap().unwrap();
    assert_eq!(tail.metadata, json!({"tail": true})); // tail replayed from the WAL
}

#[test]
fn crash_during_snapshot_write_falls_back() {
    // Models a crash after records.snap is durable but before the manifest (the
    // commit marker) is renamed in. No manifest => the reader ignores the snapshot.
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
        seed(&mut db, "c", 60);
        db.compact().unwrap();
        db.close().unwrap();
    }
    // records.snap stays; remove the manifest only (the unfinished-write state).
    assert!(records_path(temp.path()).exists());
    fs::remove_file(manifest_path(temp.path())).unwrap();

    let db = BicDb::open_with_config(temp.path(), snapshot_on()).unwrap();
    assert_all_present(&db, "c", 60);
}
