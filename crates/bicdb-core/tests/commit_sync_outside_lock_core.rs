//! Core-level qualification of the row-commit sync outside the page store's
//! writer lock (`docs/commit-fsync-outside-locks.md`): concurrent writers
//! with per-writer hot rows, a contended shared row, deletes and a unique
//! index, gate on. The overlap safety net must never fire (core row locks
//! cover every page key a row commit writes), every acknowledged commit must
//! be visible and survive reopen, and readers must never see a writer's hot
//! row go backwards.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;

const WRITERS: usize = 8;
const COMMITS: u64 = 80;

fn config(gate: bool) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_sync_outbox(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_commit_sync_outside_lock(gate)
}

fn index(name: &str, field: &str, unique: bool) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        collection: "rows".into(),
        fields: vec![IndexField::MetadataPath(vec![field.into()])],
        unique,
        nulls_not_distinct: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    }
}

fn seq_of(record: &Record) -> u64 {
    record.metadata["seq"].as_u64().unwrap_or(0)
}

fn workload(gate: bool) {
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), config(gate)).unwrap();
    db.create_collection("rows").unwrap();
    db.create_index(index("rows_owner", "owner", false)).unwrap();
    db.create_index(index("rows_uk", "uk", true)).unwrap();
    {
        let mut seed = db.begin_transaction().unwrap();
        for writer in 0..WRITERS {
            seed.insert(
                "rows",
                Record::new(format!("hot-{writer}"))
                    .with_metadata(json!({"owner": writer, "seq": 0, "uk": format!("h-{writer}")})),
            )
            .unwrap();
        }
        seed.insert(
            "rows",
            Record::new("shared").with_metadata(json!({"owner": 99, "seq": 0, "uk": "shared"})),
        )
        .unwrap();
        seed.commit().unwrap();
    }
    let db = Arc::new(RwLock::new(db));
    let done = AtomicBool::new(false);
    let shared_acks = Mutex::new(Vec::<String>::new());
    std::thread::scope(|scope| {
        let mut writers = Vec::new();
        for writer in 0..WRITERS {
            let db = &db;
            let shared_acks = &shared_acks;
            writers.push(scope.spawn(move || {
                for i in 1..=COMMITS {
                    let guard = db.read().unwrap();
                    let mut tx = guard.begin_transaction().unwrap();
                    tx.insert(
                        "rows",
                        Record::new(format!("r-{writer}-{i}")).with_metadata(
                            json!({"owner": writer, "seq": i, "uk": format!("u-{writer}-{i}")}),
                        ),
                    )
                    .unwrap();
                    tx.update(
                        "rows",
                        Record::new(format!("hot-{writer}")).with_metadata(
                            json!({"owner": writer, "seq": i, "uk": format!("h-{writer}")}),
                        ),
                    )
                    .unwrap();
                    if i % 5 == 0 {
                        tx.delete("rows", &format!("r-{writer}-{}", i - 2)).unwrap();
                    }
                    tx.commit().unwrap();
                    // The contended row: retried on a (legitimate, core-level)
                    // write conflict.
                    if i % 4 == 0 {
                        let tag = format!("{writer}-{i}");
                        for _ in 0..200 {
                            let mut tx = guard.begin_transaction().unwrap();
                            let wrote = tx.update(
                                "rows",
                                Record::new("shared").with_metadata(
                                    json!({"owner": 99, "seq": i, "uk": "shared", "tag": tag}),
                                ),
                            );
                            if wrote.is_ok() && tx.commit().is_ok() {
                                shared_acks.lock().unwrap().push(tag.clone());
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                }
            }));
        }
        // Readers: a writer's hot row never goes backwards.
        let reader = scope.spawn(|| {
            let mut last = vec![0u64; WRITERS];
            let mut reads = 0u64;
            while !done.load(Ordering::Acquire) {
                let guard = db.read().unwrap();
                for (writer, last) in last.iter_mut().enumerate() {
                    let record = guard.get("rows", &format!("hot-{writer}")).unwrap().unwrap();
                    let seq = seq_of(&record);
                    assert!(seq >= *last, "hot-{writer} went back from {last} to {seq}");
                    *last = seq;
                    reads += 1;
                }
            }
            reads
        });
        for handle in writers {
            handle.join().unwrap();
        }
        done.store(true, Ordering::Release);
        assert!(reader.join().unwrap() > 0);
    });
    let db = Arc::try_unwrap(db).ok().unwrap().into_inner().unwrap();
    if let Some(paged) = db.paged_records_handle() {
        let stats = paged.store().committing_stats();
        if gate {
            assert!(stats.enqueued > (WRITERS as u64) * COMMITS, "{stats:?}");
            assert_eq!(stats.published, stats.enqueued, "{stats:?}");
            assert_eq!(stats.depth, 0, "{stats:?}");
            assert_eq!(stats.overlap_waits, 0, "the overlap safety net fired: {stats:?}");
        } else {
            assert_eq!(stats, Default::default());
        }
    }
    let shared_acks = shared_acks.into_inner().unwrap();
    check(&db, &shared_acks);
    drop(db);
    let db = BicDb::open_with_config(temp.path(), config(gate)).unwrap();
    check(&db, &shared_acks);
}

fn check(db: &BicDb, shared_acks: &[String]) {
    for writer in 0..WRITERS {
        let hot = db.get("rows", &format!("hot-{writer}")).unwrap().unwrap();
        assert_eq!(seq_of(&hot), COMMITS);
        for i in 1..=COMMITS {
            let deleted = (i + 2) % 5 == 0 && i + 2 <= COMMITS;
            let row = db.get("rows", &format!("r-{writer}-{i}")).unwrap();
            assert_eq!(row.is_none(), deleted, "r-{writer}-{i}");
        }
    }
    let shared = db.get("rows", "shared").unwrap().unwrap();
    let tag = shared.metadata["tag"].as_str().unwrap_or_default().to_string();
    assert!(
        shared_acks.iter().any(|ack| *ack == tag),
        "the shared row holds an acknowledged update, not {tag:?}"
    );
}

#[test]
fn concurrent_row_commits_with_the_gate_on() {
    workload(true);
}

#[test]
fn concurrent_row_commits_with_the_gate_off() {
    workload(false);
}
