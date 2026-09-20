//! Engine-level scaling benchmark for the concurrent-commit path (NOT a microbench
//! of structural costs like `mvcc_concurrency_probe`, but the REAL BicDb commit
//! path: begin -> insert indexed rows -> shared commit -> durable WAL).
//!
//! Each thread owns a disjoint "warehouse" and repeatedly commits a transaction
//! that INSERTS a batch of new, per-warehouse-indexed rows (an order-line-like
//! workload: brand-new ids never conflict; new index keys land in the warehouse's
//! index shard). This exercises everything the concurrent path touches: the
//! per-collection record shards (3b), the value-sharded secondary index + its
//! per-shard locks (task #2), and the sharded write_locks/tx_states/
//! active_snapshots orchestration (task #3).
//!
//! Config is chosen by env so the SAME binary A/Bs each ingredient:
//!   BICDB_INDEX_STORE = btree (default) | sharded     (task #2 on/off)
//!   BICDB_ORCH_SHARDS = 1 (global) | 16 (sharded)     (task #3 on/off)
//! Run it under each combination and compare commits/sec scaling 1->16 threads.
//! fsync is OFF (tmpfs-equivalent) so we measure lock/CPU/concurrency, not disk.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bicdb_core::{BicDb, DbConfig, IndexDefinition, IndexField, IndexKind, Record};
use serde_json::json;

const ROWS_PER_COMMIT: usize = 10;
const COMMITS_PER_THREAD: u64 = 20_000;

fn run(threads: usize) -> f64 {
    let dir = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(dir.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("ol").unwrap();
    // Secondary B-tree index whose LEADING field is the warehouse id -- value-based
    // sharding distributes warehouses across index shards (the keystone), so
    // disjoint-warehouse commits write disjoint index shards.
    db.create_index(IndexDefinition {
        name: "idx_ol_w_n".to_string(),
        collection: "ol".to_string(),
        fields: vec![
            IndexField::MetadataPath(vec!["w".to_string()]),
            IndexField::MetadataPath(vec!["n".to_string()]),
        ],
        unique: false,
        kind: IndexKind::BTree,
        predicate: None,
        exclusion: None,
    })
    .unwrap();

    let db = Arc::new(db);
    // Per-thread read-your-writes floor (mirrors pgwire's per-connection floor) so a
    // thread never self-conflicts against the lagging contiguous watermark.
    let start = Instant::now();
    std::thread::scope(|scope| {
        for w in 0..threads {
            let db = Arc::clone(&db);
            scope.spawn(move || {
                let floor = AtomicU64::new(0);
                for n in 0..COMMITS_PER_THREAD {
                    let mut tx = db
                        .begin_transaction_after(floor.load(Ordering::Relaxed))
                        .unwrap();
                    for k in 0..ROWS_PER_COMMIT {
                        tx.insert(
                            "ol",
                            Record::new(format!("{w}-{n}-{k}"))
                                .with_metadata(json!({ "w": w, "n": n, "k": k, "amt": 1 })),
                        )
                        .unwrap();
                    }
                    tx.prepare_wal_payloads();
                    let seq = db.commit_buffered_transaction(&mut tx).unwrap();
                    db.tx_log_handle().write_durable(seq).unwrap();
                    floor.store(seq, Ordering::Relaxed);
                }
            });
        }
    });
    let total = threads as u64 * COMMITS_PER_THREAD;
    total as f64 / start.elapsed().as_secs_f64()
}

fn main() {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    let store = std::env::var("BICDB_INDEX_STORE").unwrap_or_else(|_| "btree".into());
    let orch = std::env::var("BICDB_ORCH_SHARDS").unwrap_or_else(|_| "16".into());
    println!(
        "concurrent_commit_bench (cores={cores}, index_store={store}, orch_shards={orch}, \
         {ROWS_PER_COMMIT} inserts/commit, {COMMITS_PER_THREAD} commits/thread)"
    );
    println!("{:>7} | {:>12} | {:>8}", "threads", "commits/s", "scale");
    println!("{}", "-".repeat(34));
    let mut base = 0.0;
    for &t in &[1usize, 2, 4, 8, 16] {
        let cps = run(t);
        if t == 1 {
            base = cps;
        }
        println!("{t:>7} | {cps:>12.0} | {:>6.2}x", cps / base);
    }
}
