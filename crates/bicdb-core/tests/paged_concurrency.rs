//! Concurrent commits through `BicDb` against `storage_mode = server_paged`.
//!
//! `bicdb-page`'s own tests cover concurrency at the engine level. This covers
//! the layer above it, where a core transaction's writes are mirrored into a
//! *separate* paged transaction after the core has already decided the commit
//! succeeds — the seam where the two concurrency-control schemes meet.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use bicdb_core::{BicDb, CompactionOptions, DbConfig, Record, StorageMode};
use serde_json::json;

fn config(mode: StorageMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(mode)
}

fn disjoint_rows_all_commit(mode: StorageMode) {
    let temp = tempfile::tempdir().unwrap();
    let committers = 6usize;
    let per = 60usize;
    let rows = 4usize;

    let mut db = BicDb::open_with_config(temp.path(), config(mode.clone())).unwrap();
    db.create_collection("ledger").unwrap();
    {
        let mut seed = db.begin_transaction().unwrap();
        for c in 0..committers {
            for r in 0..rows {
                seed.insert(
                    "ledger",
                    Record::new(format!("c{c}-r{r}")).with_metadata(json!({ "v": 0 })),
                )
                .unwrap();
            }
        }
        seed.commit().unwrap();
    }

    let db = Arc::new(RwLock::new(db));
    // Collect failures rather than panicking inside the threads, so the test
    // reports *what* went wrong instead of dying on the first `expect`.
    let failures = Arc::new(RwLock::new(Vec::<String>::new()));
    let completed = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|scope| {
        for c in 0..committers {
            let db = Arc::clone(&db);
            let failures = Arc::clone(&failures);
            let completed = Arc::clone(&completed);
            scope.spawn(move || {
                for i in 0..per {
                    let r = i % rows;
                    let value = (i + 1) as i64;
                    let guard = db.read().unwrap();
                    let mut tx = guard.begin_transaction().unwrap();
                    tx.update(
                        "ledger",
                        Record::new(format!("c{c}-r{r}")).with_metadata(json!({ "v": value })),
                    )
                    .unwrap();
                    if let Err(error) = tx.commit() {
                        failures
                            .write()
                            .unwrap()
                            .push(format!("c{c}-r{r} i={i}: {error}"));
                        return;
                    }
                    completed.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
    });

    let failures = failures.read().unwrap();
    assert!(
        failures.is_empty(),
        "[{mode}] {} commits failed on disjoint rows ({} of {} succeeded); first few: {:?}",
        failures.len(),
        completed.load(Ordering::SeqCst),
        committers * per,
        &failures[..failures.len().min(5)]
    );

    // Each committer's last write to each row must be what is readable.
    let db = db.read().unwrap();
    let last_i_for = |r: usize| (0..per).rev().find(|i| i % rows == r).unwrap();
    for c in 0..committers {
        for r in 0..rows {
            let id = format!("c{c}-r{r}");
            let record = db
                .get("ledger", &id)
                .unwrap()
                .unwrap_or_else(|| panic!("{id} missing after concurrent commits"));
            assert_eq!(
                record.metadata["v"],
                json!(last_i_for(r) as i64 + 1),
                "{id} holds a stale value: the paged apply landed out of order"
            );
        }
    }
}

fn commits_survive_compaction(mode: StorageMode) {
    // The online checkpoint runs under the collection write guard while
    // committers hold read guards, so it interleaves with the paged apply.
    let temp = tempfile::tempdir().unwrap();
    let committers = 4usize;
    let per = 40usize;

    let mut db = BicDb::open_with_config(temp.path(), config(mode.clone())).unwrap();
    db.create_collection("ledger").unwrap();
    {
        let mut seed = db.begin_transaction().unwrap();
        for c in 0..committers {
            seed.insert(
                "ledger",
                Record::new(format!("c{c}")).with_metadata(json!({ "v": 0 })),
            )
            .unwrap();
        }
        seed.commit().unwrap();
    }

    let db = Arc::new(RwLock::new(db));
    let failures = Arc::new(RwLock::new(Vec::<String>::new()));
    let done = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|scope| {
        {
            let db = Arc::clone(&db);
            let done = Arc::clone(&done);
            let failures = Arc::clone(&failures);
            scope.spawn(move || {
                let options = CompactionOptions {
                    force: true,
                    allow_pending: true,
                    ..Default::default()
                };
                // Deadline as well as a completion count: a committer that
                // returns early must not leave this thread spinning forever,
                // which would hang the scope and swallow the real failure.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                while done.load(Ordering::SeqCst) < committers
                    && std::time::Instant::now() < deadline
                {
                    let result = db.write().unwrap().compact_with_options(options.clone());
                    if let Err(error) = result {
                        failures
                            .write()
                            .unwrap()
                            .push(format!("compaction: {error}"));
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            });
        }
        for c in 0..committers {
            let db = Arc::clone(&db);
            let failures = Arc::clone(&failures);
            let done = Arc::clone(&done);
            scope.spawn(move || {
                for i in 0..per {
                    let guard = db.read().unwrap();
                    let mut tx = guard.begin_transaction().unwrap();
                    tx.update(
                        "ledger",
                        Record::new(format!("c{c}")).with_metadata(json!({ "v": i + 1 })),
                    )
                    .unwrap();
                    if let Err(error) = tx.commit() {
                        failures
                            .write()
                            .unwrap()
                            .push(format!("c{c} i={i}: {error}"));
                        break;
                    }
                }
                done.fetch_add(1, Ordering::SeqCst);
            });
        }
    });

    let failures = failures.read().unwrap();
    assert!(
        failures.is_empty(),
        "[{mode}] {} operations failed while compaction ran; first few: {:?}",
        failures.len(),
        &failures[..failures.len().min(5)]
    );
}

// Both modes, so a failure says whether the paged engine introduced it or the
// core already had it. A bug that reproduces in `embedded_memory` too is not a
// paged-storage bug, and chasing it as one wastes the search.

#[test]
fn disjoint_rows_all_commit_embedded() {
    disjoint_rows_all_commit(StorageMode::EmbeddedMemory);
}

#[test]
fn disjoint_rows_all_commit_paged() {
    disjoint_rows_all_commit(StorageMode::ServerPaged);
}

#[test]
fn commits_survive_compaction_embedded() {
    commits_survive_compaction(StorageMode::EmbeddedMemory);
}

#[test]
fn commits_survive_compaction_paged() {
    commits_survive_compaction(StorageMode::ServerPaged);
}

/// A commit that returned `Ok` must survive the process going away without
/// `close()`.
///
/// Every other paged test closes the database first, so this path — the one that
/// actually happens when a process is killed — was untested through `BicDb`.
/// `close()` being required for durability would mean the WAL is decorative.
fn commits_survive_drop_without_close(mode: StorageMode) {
    for count in [1usize, 250] {
        let temp = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open_with_config(temp.path(), config(mode.clone())).unwrap();
            db.create_collection("ledger").unwrap();
            let mut tx = db.begin_transaction().unwrap();
            for index in 0..count {
                tx.insert(
                    "ledger",
                    Record::new(format!("committed-{index:04}"))
                        .with_metadata(json!({ "index": index })),
                )
                .unwrap();
            }
            tx.commit().unwrap();
            drop(db); // no close(): the crash-shaped exit
        }

        let db = BicDb::open_with_config(temp.path(), config(mode.clone())).unwrap();
        assert_eq!(
            db.scan_collection("ledger").unwrap().len(),
            count,
            "[{mode}] {count} rows committed before an unclosed drop, \
             {} readable after reopen",
            db.scan_collection("ledger").unwrap().len()
        );
    }
}

#[test]
fn commits_survive_drop_without_close_embedded() {
    commits_survive_drop_without_close(StorageMode::EmbeddedMemory);
}

#[test]
fn commits_survive_drop_without_close_paged() {
    commits_survive_drop_without_close(StorageMode::ServerPaged);
}

/// As above, but the collection is created — and a transaction abandoned — in an
/// *earlier* database instance than the one that commits.
///
/// This is the shape `crash_boundary_recovery_handles_small_and_large_transactions`
/// exercises, and it is not the same test as committing into a collection this
/// instance created: the catalog is replayed rather than freshly written, so a
/// commit lands against a collection whose paged state came back from disk.
fn commits_survive_when_the_collection_predates_the_writer(mode: StorageMode) {
    for count in [1usize, 250] {
        let temp = tempfile::tempdir().unwrap();
        {
            let mut db = BicDb::open_with_config(temp.path(), config(mode.clone())).unwrap();
            db.create_collection("patients").unwrap();
            let mut tx = db.begin_transaction().unwrap();
            for index in 0..count {
                tx.insert("patients", Record::new(format!("pending-{index:04}")))
                    .unwrap();
            }
            drop(tx); // abandoned, never committed
            drop(db);
        }
        {
            let db = BicDb::open_with_config(temp.path(), config(mode.clone())).unwrap();
            assert_eq!(
                db.scan_collection("patients").unwrap().len(),
                0,
                "[{mode}] an abandoned transaction became visible"
            );
        }
        {
            let db = BicDb::open_with_config(temp.path(), config(mode.clone())).unwrap();
            let mut tx = db.begin_transaction().unwrap();
            for index in 0..count {
                tx.insert("patients", Record::new(format!("committed-{index:04}")))
                    .unwrap();
            }
            tx.commit().unwrap();
            drop(db);
        }
        let db = BicDb::open_with_config(temp.path(), config(mode.clone())).unwrap();
        let found = db.scan_collection("patients").unwrap().len();
        assert_eq!(
            found, count,
            "[{mode}] committed {count} rows into a pre-existing collection, {found} survived"
        );
    }
}

#[test]
fn commits_survive_when_the_collection_predates_the_writer_embedded() {
    commits_survive_when_the_collection_predates_the_writer(StorageMode::EmbeddedMemory);
}

#[test]
fn commits_survive_when_the_collection_predates_the_writer_paged() {
    commits_survive_when_the_collection_predates_the_writer(StorageMode::ServerPaged);
}

/// Logical logging: in paged mode the core transaction log carries a tiny
/// commit marker per transaction, not the records — the page store's own WAL
/// is the durable home for rows. Before this, every commit wrote its records
/// TWICE (core log + page WAL), doubling ingest write volume.
///
/// The size bound is deliberately generous (markers are ~100 bytes; records
/// here are ~300): a regression back to full-record logging blows through it
/// by an order of magnitude, while framing/compression changes will not.
#[test]
fn the_core_log_carries_markers_not_records_in_paged_mode() {
    let temp = tempfile::tempdir().unwrap();
    // fsync on: commits wait for the log writer, so the size measured below
    // reflects what was actually logged rather than an undrained buffer.
    let db = BicDb::open_with_config(
        temp.path(),
        config(StorageMode::ServerPaged).with_fsync(true),
    )
    .unwrap();
    {
        let mut db = db;
        db.create_collection("bulk").unwrap();
        let padding = "x".repeat(300);
        for batch in 0..10 {
            let mut tx = db.begin_transaction().unwrap();
            for index in 0..100 {
                tx.insert(
                    "bulk",
                    Record::new(format!("r-{batch}-{index}"))
                        .with_metadata(json!({ "pad": padding })),
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        // Rows must be durable and readable straight through the page store.
        assert_eq!(db.scan_collection("bulk").unwrap().len(), 1_000);

        let log_bytes = std::fs::metadata(temp.path().join("transactions.log"))
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        // 1,000 records x ~350 bytes would be ~350 KB if records were logged;
        // 10 markers are well under 4 KB even with framing.
        assert!(
            log_bytes < 16 * 1024,
            "core transaction log is {log_bytes} bytes for 10 marker commits — \
             records are being double-logged again"
        );
        drop(db); // crash-shaped: recovery must come from the page WAL
    }

    let db = BicDb::open_with_config(temp.path(), config(StorageMode::ServerPaged)).unwrap();
    assert_eq!(
        db.scan_collection("bulk").unwrap().len(),
        1_000,
        "rows lost across reopen: paged recovery did not cover what the \
         marker-only core log no longer replays"
    );
    assert!(db.get("bulk", "r-9-99").unwrap().is_some());
}

/// The replication exception to marker logging: standbys are fed Write frames
/// out of the retained core log, so a REPLICATING paged primary must keep
/// logging full records. Mutation check for the gate itself — remove the
/// `!replication.enabled` condition and this fails.
#[test]
fn a_replicating_paged_primary_still_logs_full_records() {
    let temp = tempfile::tempdir().unwrap();
    let mut cfg = config(StorageMode::ServerPaged).with_fsync(true);
    cfg.replication.enabled = true;
    cfg.replication.mode = bicdb_core::ReplicationMode::Primary;
    cfg.replication.listen_addr = Some("127.0.0.1:0".to_string());
    cfg.replication.tls = Some(bicdb_core::ReplicationTlsConfig {
        cert_path: std::path::PathBuf::new(),
        key_path: std::path::PathBuf::new(),
        ca_path: std::path::PathBuf::new(),
        require_client_cert: false,
        dev_localhost_plaintext: true,
    });
    let mut db = BicDb::open_with_config(temp.path(), cfg).unwrap();
    db.create_collection("bulk").unwrap();
    let padding = "x".repeat(300);
    let mut tx = db.begin_transaction().unwrap();
    for index in 0..100 {
        tx.insert(
            "bulk",
            Record::new(format!("r-{index}")).with_metadata(json!({ "pad": padding })),
        )
        .unwrap();
    }
    tx.commit().unwrap();
    // Measure BEFORE close — close checkpoints and truncates the log, which
    // would hide the difference (fsync-on commits already drained the writer).
    let log_bytes = std::fs::metadata(temp.path().join("transactions.log"))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    assert!(
        log_bytes > 30 * 1024,
        "core log is only {log_bytes} bytes — a replicating primary switched \
         to marker logging and would starve its standbys"
    );
}

/// A paged vector collection must survive reopen WITHOUT relying on core-log
/// Write-frame replay (logical logging removed that accidental cover). The
/// transactional apply path sets `vector_dim` on a None -> Some transition but
/// historically never persisted the catalog, so the collection reopened as
/// LAZY (zero resident rows) and `load_hnsw_indexes` tombstoned every node of
/// the persisted graph against the empty resident set.
#[test]
fn hnsw_search_survives_reopen_without_close_in_paged_mode() {
    let temp = tempfile::tempdir().unwrap();
    {
        let mut db =
            BicDb::open_with_config(temp.path(), config(StorageMode::ServerPaged)).unwrap();
        db.create_collection("memories").unwrap();
        // batch_insert commits through the TRANSACTIONAL apply — the path that
        // lost the dimension. A plain insert() would mask the bug.
        db.batch_insert(
            "memories",
            [
                Record::new("keep").with_vector(vec![1.0, 0.0]),
                Record::new("delete").with_vector(vec![0.99, 0.01]),
                Record::new("far").with_vector(vec![0.0, 1.0]),
            ],
        )
        .unwrap();
        db.create_vector_index("memories", bicdb_core::HnswIndexConfig::default())
            .unwrap();
        assert!(db.delete("memories", "delete").unwrap());
        // Drop WITHOUT close: close() would checkpoint and persist the catalog
        // itself, hiding the missing commit-path persist.
    }
    let db = BicDb::open_with_config(temp.path(), config(StorageMode::ServerPaged)).unwrap();
    let report = db.verify_vector_index("memories").unwrap();
    assert!(report.valid);
    assert_eq!(
        (report.live_vectors, report.tombstoned_vectors),
        (2, 1),
        "persisted HNSW nodes were tombstoned at load: the collection reopened \
         without resident vectors (vector_dim missing from the durable catalog)"
    );
    let results = db
        .search_vector_ann("memories", &[1.0, 0.0], 3, 16)
        .unwrap();
    assert_eq!(results[0].record.id, "keep");
    assert!(!results.iter().any(|result| result.record.id == "delete"));
}

/// Ad-hoc replication export from a marker-logged paged database must refuse
/// loudly, not export empty commits that silently starve a standby.
#[test]
fn replication_export_refuses_on_a_marker_logged_paged_database() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(temp.path(), config(StorageMode::ServerPaged)).unwrap();
    db.create_collection("items").unwrap();
    db.insert("items", Record::new("row").with_metadata(json!({})))
        .unwrap();
    let error = db
        .export_replication_frames_since(0, 10)
        .expect_err("marker-logged paged export must refuse");
    assert!(
        error.to_string().contains("materialized commit markers"),
        "unexpected error: {error}"
    );
}

/// Slice 3: a vector collection reopens LAZY — zero resident rows — and both
/// ANN and exact search answer from pages. Parity oracle: the same corpus in
/// embedded mode.
#[test]
fn vector_search_serves_from_pages_with_zero_resident_rows() {
    let paged_dir = tempfile::tempdir().unwrap();
    let embedded_dir = tempfile::tempdir().unwrap();
    let embedded_config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::EmbeddedMemory);
    let seed = |db: &mut BicDb| {
        db.create_collection("passages").unwrap();
        let records: Vec<Record> = (0..300)
            .map(|index| {
                let angle = index as f32 * 0.021;
                Record::new(format!("p{index:04}"))
                    .with_vector(vec![angle.cos(), angle.sin(), (index % 7) as f32 * 0.1])
                    .with_metadata(json!({ "n": index }))
            })
            .collect();
        db.batch_insert("passages", records).unwrap();
        db.create_vector_index("passages", bicdb_core::HnswIndexConfig::default())
            .unwrap();
        assert!(db.delete("passages", "p0033").unwrap());
    };
    {
        let mut db =
            BicDb::open_with_config(paged_dir.path(), config(StorageMode::ServerPaged)).unwrap();
        seed(&mut db);
        // Drop WITHOUT close: reopen must trust pages alone.
    }
    let mut embedded = BicDb::open_with_config(embedded_dir.path(), embedded_config).unwrap();
    seed(&mut embedded);

    let paged =
        BicDb::open_with_config(paged_dir.path(), config(StorageMode::ServerPaged)).unwrap();
    let resident = paged
        .residency_report()
        .unwrap()
        .collections
        .iter()
        .find(|entry| entry.name == "passages")
        .map(|entry| entry.record_count)
        .unwrap_or(0);
    assert_eq!(
        resident, 0,
        "vector collection must reopen with no resident rows"
    );

    let query = [0.7f32, 0.7, 0.2];
    let paged_ann: Vec<String> = paged
        .search_vector_ann("passages", &query, 5, 32)
        .unwrap()
        .into_iter()
        .map(|result| result.record.id)
        .collect();
    let embedded_ann: Vec<String> = embedded
        .search_vector_ann("passages", &query, 5, 32)
        .unwrap()
        .into_iter()
        .map(|result| result.record.id)
        .collect();
    assert_eq!(
        paged_ann, embedded_ann,
        "ANN diverged from the embedded oracle"
    );
    assert!(!paged_ann.contains(&"p0033".to_string()));

    let paged_exact: Vec<String> = paged
        .search_vector_exact("passages", &query, 5)
        .unwrap()
        .into_iter()
        .map(|result| result.record.id)
        .collect();
    let embedded_exact: Vec<String> = embedded
        .search_vector_exact("passages", &query, 5)
        .unwrap()
        .into_iter()
        .map(|result| result.record.id)
        .collect();
    assert_eq!(
        paged_exact, embedded_exact,
        "exact search diverged from the embedded oracle"
    );
    assert!(paged.verify_vector_index("passages").unwrap().valid);
}
