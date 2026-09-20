//! De-risk probe for fine-grained MVCC + concurrent commit/apply (PG-style).
//!
//! It does NOT touch bicdb's real commit path. It models the *structural* costs
//! of the proposed Phase-1 design and the single-global-lock baseline, so we can
//! measure the one question that sank the prior 4 attempts BEFORE a multi-week
//! rewrite: does per-record-latched concurrent commit actually scale, or does the
//! per-op latch overhead + the global per-index B-tree apply lock eat the gain?
//!
//! Model of one TPC-C-NewOrder-ish commit (faithful to what COMMIT_TRACE showed):
//!   * ~12 record upserts -> push a version onto that record's slot
//!     (apply_records, ~21% of the real serial section)
//!   * 4 GLOBAL secondary indexes (one B-tree each, shared across warehouses):
//!     read-lock validate probe, then write-lock apply insert
//!     (index_build+validate+apply_index, ~70% of the real serial section)
//!
//! Two execution models, each run at 1/2/4/8/16 threads on DISJOINT warehouses
//! (the friendliest case for concurrency -- disjoint rows, like the benchmark):
//!   GLOBAL: hold one Mutex for the whole commit (today's commit_lock).
//!   FINE  : sharded record store (per-bucket Mutex) + per-index RwLock (the
//!           proposed fine-grained design). commit_seq via a plain atomic.
//!
//! Read the result: if FINE throughput scales ~linearly with threads and beats
//! GLOBAL, the rewrite has headroom. If FINE plateaus near GLOBAL (because every
//! warehouse's commit still serializes on the shared per-index write lock), the
//! index apply is the irreducible wall and the rewrite cannot reach the goal.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

const RECORDS_PER_COMMIT: usize = 12;
const INDEXES: usize = 4;
const RECORD_BUCKETS: usize = 1024;
const INDEX_SHARDS: usize = 16;
const ORCH_SHARDS: usize = 16;
const COMMITS_PER_THREAD: u64 = 40_000;

// Models bicdb's real commit orchestration -- the GLOBAL mutexes that sank 3b:
// write_locks (per-write intent), tx_states, active_snapshots, the AppliedWatermark,
// and the WAL pending queue. `sharded` shards write_locks/tx_states/active_snapshots
// per warehouse; the watermark and WAL queue are inherently global (contiguous
// commit_seq order) and stay single-mutex either way.
struct Orch {
    write_locks: Vec<Mutex<HashMap<(u32, u64), u64>>>,
    tx_states: Vec<Mutex<HashMap<u64, u8>>>,
    active_snapshots: Vec<Mutex<BTreeMap<u64, u64>>>,
    watermark: Mutex<(BTreeSet<u64>, u64)>, // (pending out-of-order seqs, contiguous hi)
    wal_pending: Mutex<BTreeMap<u64, ()>>,
}

impl Orch {
    fn new(shards: usize) -> Self {
        Orch {
            write_locks: (0..shards).map(|_| Mutex::new(HashMap::new())).collect(),
            tx_states: (0..shards).map(|_| Mutex::new(HashMap::new())).collect(),
            active_snapshots: (0..shards).map(|_| Mutex::new(BTreeMap::new())).collect(),
            watermark: Mutex::new((BTreeSet::new(), 0)),
            wal_pending: Mutex::new(BTreeMap::new()),
        }
    }

    // One transaction's orchestration: begin (snapshot+state), per-write intent
    // locks, then commit (state, WAL enqueue, watermark advance, lock release).
    fn run_txn(&self, w: usize, seq: u64) {
        let s = w % self.write_locks.len();
        self.active_snapshots[s]
            .lock()
            .unwrap()
            .entry(seq)
            .and_modify(|c| *c += 1)
            .or_insert(1);
        self.tx_states[s].lock().unwrap().insert(seq, 0); // pending
        for r in 0..RECORDS_PER_COMMIT as u64 {
            self.write_locks[s]
                .lock()
                .unwrap()
                .insert((w as u32, seq * 16 + r), seq);
        }
        // commit
        self.wal_pending.lock().unwrap().insert(seq, ());
        {
            let mut wm = self.watermark.lock().unwrap();
            wm.0.insert(seq);
            loop {
                let next = wm.1 + 1;
                if wm.0.remove(&next) {
                    wm.1 = next;
                } else {
                    break;
                }
            }
        }
        self.tx_states[s].lock().unwrap().insert(seq, 1); // committed
        self.write_locks[s]
            .lock()
            .unwrap()
            .retain(|_, owner| *owner != seq);
        self.active_snapshots[s].lock().unwrap().remove(&seq);
    }
}

type Version = (u64, Option<u64>); // (created_seq, deleted_seq) = xmin/xmax

#[derive(Default)]
struct RecordSlot {
    versions: Vec<Version>,
    max_seq: u64, // version_max_tx equivalent (O(1) conflict check)
}

fn bucket_of(key: &str) -> usize {
    // Cheap FNV-1a, same low-constant cost a real shard pick would pay.
    let mut h = 0xcbf29ce484222325u64;
    for b in key.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h as usize) & (RECORD_BUCKETS - 1)
}

// Index key: prefixed by warehouse, then a unique counter so the B-tree grows
// (realistic log(n) inserts) -- but the index itself is GLOBAL (shared lock).
fn ikey(w: usize, n: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(12);
    k.extend_from_slice(&(w as u32).to_be_bytes());
    k.extend_from_slice(&n.to_be_bytes());
    k
}

fn commit_work_fine(
    seq: u64,
    w: usize,
    n: u64,
    buckets: &[Mutex<HashMap<String, RecordSlot>>],
    indexes: &[RwLock<BTreeMap<Vec<u8>, BTreeSet<String>>>],
) {
    // apply_records: per-record latch, validate (max_seq), push version.
    for r in 0..RECORDS_PER_COMMIT {
        let key = format!("{w}:{}", n * RECORDS_PER_COMMIT as u64 + r as u64);
        let mut bucket = buckets[bucket_of(&key)].lock().unwrap();
        let slot = bucket.entry(key).or_default();
        let _conflict = slot.max_seq > seq; // O(1) conflict check (no conflict here)
        slot.versions.push((seq, None));
        slot.max_seq = seq;
    }
    // index apply: index 0 gets 10 inserts (order_line-like), others 1 each.
    for (gi, idx) in indexes.iter().enumerate() {
        let inserts = if gi == 0 { 10 } else { 1 };
        // validate under a shared READ lock (concurrent unique probe)
        {
            let g = idx.read().unwrap();
            for i in 0..inserts {
                let _ = g.get(&ikey(w, n * 16 + gi as u64 * 4 + i));
            }
        }
        // apply under the per-index WRITE lock (the suspected serialization point)
        {
            let mut g = idx.write().unwrap();
            for i in 0..inserts {
                g.entry(ikey(w, n * 16 + gi as u64 * 4 + i))
                    .or_default()
                    .insert(format!("{w}:{n}"));
            }
        }
    }
}

fn commit_work_global(
    seq: u64,
    w: usize,
    n: u64,
    records: &mut HashMap<String, RecordSlot>,
    indexes: &mut [BTreeMap<Vec<u8>, BTreeSet<String>>],
) {
    for r in 0..RECORDS_PER_COMMIT {
        let key = format!("{w}:{}", n * RECORDS_PER_COMMIT as u64 + r as u64);
        let slot = records.entry(key).or_default();
        let _conflict = slot.max_seq > seq;
        slot.versions.push((seq, None));
        slot.max_seq = seq;
    }
    for (gi, idx) in indexes.iter_mut().enumerate() {
        let inserts = if gi == 0 { 10 } else { 1 };
        for i in 0..inserts {
            let _ = idx.get(&ikey(w, n * 16 + gi as u64 * 4 + i)); // validate
        }
        for i in 0..inserts {
            idx.entry(ikey(w, n * 16 + gi as u64 * 4 + i))
                .or_default()
                .insert(format!("{w}:{n}"));
        }
    }
}

fn run_fine(threads: usize) -> f64 {
    let buckets: Arc<Vec<Mutex<HashMap<String, RecordSlot>>>> = Arc::new(
        (0..RECORD_BUCKETS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect(),
    );
    let indexes: Arc<Vec<RwLock<BTreeMap<Vec<u8>, BTreeSet<String>>>>> =
        Arc::new((0..INDEXES).map(|_| RwLock::new(BTreeMap::new())).collect());
    let seq = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    std::thread::scope(|s| {
        for w in 0..threads {
            let (buckets, indexes, seq) = (buckets.clone(), indexes.clone(), seq.clone());
            s.spawn(move || {
                for n in 0..COMMITS_PER_THREAD {
                    let cs = seq.fetch_add(1, Ordering::Relaxed);
                    commit_work_fine(cs, w, n, &buckets, &indexes);
                }
            });
        }
    });
    let total = threads as u64 * COMMITS_PER_THREAD;
    total as f64 / start.elapsed().as_secs_f64()
}

// FINE, but each global index is ALSO sharded (per-warehouse RwLock shards), so
// index writes from different warehouses don't contend. This is the upper bound
// IF indexes could be made concurrent -- which, this session proved, TPC-C's
// clustered integer keys CANNOT support (hash=scan-merge regression, range=1
// shard). Shown only to quantify what the per-index lock is costing.
fn run_fine_sharded_idx(threads: usize) -> f64 {
    let buckets: Arc<Vec<Mutex<HashMap<String, RecordSlot>>>> = Arc::new(
        (0..RECORD_BUCKETS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect(),
    );
    let indexes: Arc<Vec<Vec<RwLock<BTreeMap<Vec<u8>, BTreeSet<String>>>>>> = Arc::new(
        (0..INDEXES)
            .map(|_| {
                (0..INDEX_SHARDS)
                    .map(|_| RwLock::new(BTreeMap::new()))
                    .collect()
            })
            .collect(),
    );
    let seq = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    std::thread::scope(|s| {
        for w in 0..threads {
            let (buckets, indexes, seq) = (buckets.clone(), indexes.clone(), seq.clone());
            s.spawn(move || {
                let sh = w % INDEX_SHARDS;
                for n in 0..COMMITS_PER_THREAD {
                    let cs = seq.fetch_add(1, Ordering::Relaxed);
                    for r in 0..RECORDS_PER_COMMIT {
                        let key = format!("{w}:{}", n * RECORDS_PER_COMMIT as u64 + r as u64);
                        let mut bucket = buckets[bucket_of(&key)].lock().unwrap();
                        let slot = bucket.entry(key).or_default();
                        let _c = slot.max_seq > cs;
                        slot.versions.push((cs, None));
                        slot.max_seq = cs;
                    }
                    for (gi, idx) in indexes.iter().enumerate() {
                        let inserts = if gi == 0 { 10 } else { 1 };
                        let shard = &idx[sh]; // per-warehouse shard => no cross-warehouse contention
                        {
                            let g = shard.read().unwrap();
                            for i in 0..inserts {
                                let _ = g.get(&ikey(w, n * 16 + gi as u64 * 4 + i));
                            }
                        }
                        {
                            let mut g = shard.write().unwrap();
                            for i in 0..inserts {
                                g.entry(ikey(w, n * 16 + gi as u64 * 4 + i))
                                    .or_default()
                                    .insert(format!("{w}:{n}"));
                            }
                        }
                    }
                }
            });
        }
    });
    let total = threads as u64 * COMMITS_PER_THREAD;
    total as f64 / start.elapsed().as_secs_f64()
}

// FINE + per-warehouse sharded indexes + the commit ORCHESTRATION. orch_shards=1
// => global mutexes (3b-style); orch_shards=ORCH_SHARDS => lean/sharded.
fn run_fine_orch(threads: usize, orch_shards: usize) -> f64 {
    let buckets: Arc<Vec<Mutex<HashMap<String, RecordSlot>>>> = Arc::new(
        (0..RECORD_BUCKETS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect(),
    );
    let indexes: Arc<Vec<Vec<RwLock<BTreeMap<Vec<u8>, BTreeSet<String>>>>>> = Arc::new(
        (0..INDEXES)
            .map(|_| {
                (0..INDEX_SHARDS)
                    .map(|_| RwLock::new(BTreeMap::new()))
                    .collect()
            })
            .collect(),
    );
    let orch = Arc::new(Orch::new(orch_shards));
    let seq = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    std::thread::scope(|s| {
        for w in 0..threads {
            let (buckets, indexes, orch, seq) =
                (buckets.clone(), indexes.clone(), orch.clone(), seq.clone());
            s.spawn(move || {
                let sh = w % INDEX_SHARDS;
                for n in 0..COMMITS_PER_THREAD {
                    let cs = seq.fetch_add(1, Ordering::Relaxed);
                    for r in 0..RECORDS_PER_COMMIT {
                        let key = format!("{w}:{}", n * RECORDS_PER_COMMIT as u64 + r as u64);
                        let mut bucket = buckets[bucket_of(&key)].lock().unwrap();
                        let slot = bucket.entry(key).or_default();
                        let _c = slot.max_seq > cs;
                        slot.versions.push((cs, None));
                        slot.max_seq = cs;
                    }
                    for (gi, idx) in indexes.iter().enumerate() {
                        let inserts = if gi == 0 { 10 } else { 1 };
                        let shard = &idx[sh];
                        {
                            let g = shard.read().unwrap();
                            for i in 0..inserts {
                                let _ = g.get(&ikey(w, n * 16 + gi as u64 * 4 + i));
                            }
                        }
                        {
                            let mut g = shard.write().unwrap();
                            for i in 0..inserts {
                                g.entry(ikey(w, n * 16 + gi as u64 * 4 + i))
                                    .or_default()
                                    .insert(format!("{w}:{n}"));
                            }
                        }
                    }
                    orch.run_txn(w, cs);
                }
            });
        }
    });
    let total = threads as u64 * COMMITS_PER_THREAD;
    total as f64 / start.elapsed().as_secs_f64()
}

fn run_global(threads: usize) -> f64 {
    // One lock guarding the whole record+index state = today's commit_lock.
    let state = Arc::new(Mutex::new((
        HashMap::<String, RecordSlot>::new(),
        (0..INDEXES).map(|_| BTreeMap::new()).collect::<Vec<_>>(),
    )));
    let seq = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    std::thread::scope(|s| {
        for w in 0..threads {
            let (state, seq) = (state.clone(), seq.clone());
            s.spawn(move || {
                for n in 0..COMMITS_PER_THREAD {
                    let cs = seq.fetch_add(1, Ordering::Relaxed);
                    let mut g = state.lock().unwrap();
                    let (records, indexes) = &mut *g;
                    commit_work_global(cs, w, n, records, indexes);
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
    println!("MVCC concurrency probe (cores={cores}, {COMMITS_PER_THREAD} commits/thread, {RECORDS_PER_COMMIT} recs + {INDEXES} global indexes/commit)\n");
    println!(
        "{:>7} | {:>10} | {:>10} | {:>12} | {:>14} | {:>14}",
        "threads", "GLOBAL", "FINE", "FINE+shardIdx", "+leanOrch", "+globalOrch"
    );
    println!("{}", "-".repeat(82));
    let (mut lo1, mut go1) = (0.0, 0.0);
    for &t in &[1usize, 2, 4, 8, 16] {
        let g = run_global(t);
        let f = run_fine(t);
        let fs = run_fine_sharded_idx(t);
        let lo = run_fine_orch(t, ORCH_SHARDS);
        let go = run_fine_orch(t, 1);
        if t == 1 {
            lo1 = lo;
            go1 = go;
        }
        println!(
            "{:>7} | {:>10.0} | {:>10.0} | {:>12.0} | {:>8.0} ({:>4.1}x) | {:>8.0} ({:>4.1}x)",
            t,
            g,
            f,
            fs,
            lo,
            lo / lo1,
            go,
            go / go1
        );
    }
    println!("\nGLOBAL=commit_lock today. FINE=per-record latch (indexes stay global).");
    println!("FINE+shardIdx=also shard indexes per-warehouse. +leanOrch=add commit");
    println!("orchestration with SHARDED write_locks/tx_states/snapshots (watermark+WAL");
    println!("stay global). +globalOrch=all orchestration on single mutexes (3b-style).");
    println!("\nVERDICT: if +leanOrch keeps scaling, the rewrite has headroom and the");
    println!("orchestration can be made cheap. If +leanOrch collapses toward +globalOrch,");
    println!("the global watermark/WAL mutex is the wall (3b's grave) -- needs redesign.");
}
