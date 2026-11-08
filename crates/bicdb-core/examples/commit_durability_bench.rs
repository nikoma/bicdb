//! COMMIT latency, throughput, log growth and recovery time for the production
//! (`all-logs`) and experimental (`transaction-log`) commit durability paths
//! in `server_paged`, with fsync ON. See `docs/commit-durability-invariants.md`.
//!
//! Usage:
//!   commit_durability_bench run <db-dir> <mode> <threads> <seconds> [crash]
//!   commit_durability_bench recover <db-dir> <mode>
//!
//! `run` commits small transactions (one insert with a unique and a B-tree
//! index entry, plus an update of the thread's hot row) from `threads`
//! writers for `seconds`, then prints one JSON line: commits, commits/s,
//! p50/p95/p99/max COMMIT latency in microseconds, and log sizes. With
//! `crash` it then aborts without closing, leaving the state a crash leaves;
//! `recover` opens that state and prints the open time.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record, StorageMode};
use serde_json::json;

fn config(mode: &str) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_sync_outbox(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_transaction_log_commit_durability(mode == "transaction-log")
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

fn file_bytes(path: &Path) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

fn page_wal_bytes(db_dir: &Path) -> u64 {
    std::fs::read_dir(db_dir.join("paged"))
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("store.wal"))
        .map(|entry| entry.metadata().map(|meta| meta.len()).unwrap_or(0))
        .sum()
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn run(db_dir: &Path, mode: &str, threads: usize, seconds: u64, crash: bool) {
    let mut db = BicDb::open_with_config(db_dir, config(mode)).expect("open");
    db.create_collection("rows").expect("create");
    db.create_index(index("rows_owner", "owner", false)).expect("index");
    db.create_index(index("rows_uk", "uk", true)).expect("unique");
    for thread in 0..threads {
        let mut tx = db.begin_transaction().expect("begin");
        tx.insert(
            "rows",
            Record::new(format!("hot-{thread}"))
                .with_metadata(json!({"owner": thread, "seq": 0, "uk": format!("h-{thread}")})),
        )
        .expect("seed");
        tx.commit().expect("seed commit");
    }
    let db = Arc::new(db);
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let latencies: Vec<Vec<u64>> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|thread| {
                let db = db.clone();
                let stop = stop.clone();
                scope.spawn(move || {
                    let mut samples = Vec::with_capacity(1 << 16);
                    let mut i = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        i += 1;
                        let mut tx = db.begin_transaction().expect("begin");
                        tx.insert(
                            "rows",
                            Record::new(format!("r-{thread}-{i}")).with_metadata(
                                json!({"owner": thread, "seq": i, "uk": format!("u-{thread}-{i}")}),
                            ),
                        )
                        .expect("insert");
                        tx.update(
                            "rows",
                            Record::new(format!("hot-{thread}")).with_metadata(
                                json!({"owner": thread, "seq": i, "uk": format!("h-{thread}")}),
                            ),
                        )
                        .expect("update");
                        let commit_started = Instant::now();
                        tx.commit().expect("commit");
                        samples.push(commit_started.elapsed().as_micros() as u64);
                    }
                    samples
                })
            })
            .collect();
        std::thread::sleep(Duration::from_secs(seconds));
        stop.store(true, Ordering::Relaxed);
        workers.into_iter().map(|worker| worker.join().unwrap()).collect()
    });
    let elapsed = started.elapsed().as_secs_f64();
    let mut all: Vec<u64> = latencies.into_iter().flatten().collect();
    all.sort_unstable();
    let stats = db.deferred_materialize_stats();
    println!(
        "{}",
        json!({
            "mode": mode,
            "threads": threads,
            "seconds": elapsed,
            "commits": all.len(),
            "commits_per_second": all.len() as f64 / elapsed,
            "commit_latency_us": {
                "p50": percentile(&all, 50.0),
                "p95": percentile(&all, 95.0),
                "p99": percentile(&all, 99.0),
                "max": all.last().copied().unwrap_or(0),
            },
            "transaction_log_bytes": file_bytes(&db_dir.join("transactions.log")),
            "page_wal_bytes": page_wal_bytes(db_dir),
            "materializer": stats.map(|s| json!({
                "passes": s.passes, "wal_syncs": s.wal_syncs,
                "markers_written": s.markers_written, "queued": s.queued,
            })),
        })
    );
    if crash {
        // Leave exactly what a crash leaves: no close, no destructors.
        std::process::abort();
    }
    let db = Arc::try_unwrap(db).ok().expect("sole owner");
    db.close().expect("close");
}

fn recover(db_dir: &Path, mode: &str) {
    let transaction_log_bytes = file_bytes(&db_dir.join("transactions.log"));
    let page_wal_bytes = page_wal_bytes(db_dir);
    let started = Instant::now();
    let db = BicDb::open_with_config(db_dir, config(mode)).expect("recover");
    let open_ms = started.elapsed().as_secs_f64() * 1000.0;
    let rows = db.scan_collection("rows").expect("scan").len();
    println!(
        "{}",
        json!({
            "mode": mode,
            "recovery_open_ms": open_ms,
            "rows": rows,
            "transaction_log_bytes_before": transaction_log_bytes,
            "page_wal_bytes_before": page_wal_bytes,
        })
    );
    db.close().expect("close");
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("run") => run(
            Path::new(&args[1]),
            &args[2],
            args[3].parse().expect("threads"),
            args[4].parse().expect("seconds"),
            args.get(5).is_some_and(|flag| flag == "crash"),
        ),
        Some("recover") => recover(Path::new(&args[1]), &args[2]),
        _ => eprintln!("usage: commit_durability_bench run|recover ..."),
    }
}
