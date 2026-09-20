//! M0 index-structure concurrency benchmark (run on demand):
//!
//!   cargo test -p bicdb-core --release --test index_concurrency_bench \
//!       -- --ignored --nocapture
//!
//! Quantifies the concurrency win that motivates the concurrent-index work: it
//! pits the CURRENT model (one `RwLock<BTreeMap>`, so every index op serializes,
//! mirroring the index sitting under the database write lock) against a
//! lock-free ordered map (`crossbeam_skiplist::SkipMap`) on the index op mix
//! captured from a real HammerDB TPC-C run (insert-heavy, prefix-scan-heavy with
//! ~1000 ids per scan). It reports per-thread-count throughput and the speedup.
//!
//! This is decision data, not a shipping feature: if the lock-free structure
//! scales with cores while the locked BTreeMap flat-lines, a concurrent ordered
//! index is the right lever; the scan-heavy mix additionally stresses iteration,
//! which is where a B+Tree is expected to beat a skip list (informing the final
//! structure choice between them).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::RwLock;
use std::time::Instant;

use crossbeam_skiplist::SkipMap;

// Traced TPC-C index op mix (per 1M ops): insert 61.4%, prefix-scan 21.8%,
// extreme 6.0%, remove 4.9%, point 4.7%, range 1.2%. Collapsed here to the four
// structurally-distinct ops (extreme/range fold into scan/point):
//   insert 62, prefix-scan 22, point 11, remove 5  (per 100).
const KEY_SPACE_PREFIXES: u32 = 80; // (warehouse,district) groups
const CUSTOMERS_PER_PREFIX: u32 = 1000; // ~1000-id prefix scans, matching the trace
const OPS_PER_THREAD: u32 = 200_000;

fn make_key(prefix: u32, customer: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(8);
    key.extend_from_slice(&prefix.to_be_bytes());
    key.extend_from_slice(&customer.to_be_bytes());
    key
}

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Op selected from the traced distribution.
enum Op {
    Insert(Vec<u8>, String),
    PrefixScan(Vec<u8>),
    Point(Vec<u8>),
    Remove(Vec<u8>, String),
}

fn next_op(rng: &mut u64, thread: usize, seq: u32) -> Op {
    let roll = xorshift(rng) % 100;
    let prefix = (xorshift(rng) as u32) % KEY_SPACE_PREFIXES;
    if roll < 62 {
        // Insert a fresh unique customer under a prefix.
        let customer = CUSTOMERS_PER_PREFIX + seq;
        Op::Insert(make_key(prefix, customer), format!("t{thread}-{seq}"))
    } else if roll < 84 {
        // Prefix scan over a (warehouse,district) group (~1000 ids).
        Op::PrefixScan(prefix.to_be_bytes().to_vec())
    } else if roll < 95 {
        // Point lookup of an existing customer.
        let customer = (xorshift(rng) as u32) % CUSTOMERS_PER_PREFIX;
        Op::Point(make_key(prefix, customer))
    } else {
        // Remove an existing customer.
        let customer = (xorshift(rng) as u32) % CUSTOMERS_PER_PREFIX;
        Op::Remove(
            make_key(prefix, customer),
            format!("seed-{prefix}-{customer}"),
        )
    }
}

/// `RwLock<BTreeMap>` — today's model: a single lock guards the whole index, so
/// every op (read or write) serializes through it.
fn run_rwlock_btree(threads: usize) -> f64 {
    let index: RwLock<BTreeMap<Vec<u8>, BTreeSet<String>>> = RwLock::new(BTreeMap::new());
    {
        let mut guard = index.write().unwrap();
        for prefix in 0..KEY_SPACE_PREFIXES {
            for customer in 0..CUSTOMERS_PER_PREFIX {
                guard
                    .entry(make_key(prefix, customer))
                    .or_default()
                    .insert(format!("seed-{prefix}-{customer}"));
            }
        }
    }
    let sink = AtomicU64::new(0);
    let start = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let index = &index;
            let sink = &sink;
            scope.spawn(move || {
                let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (thread as u64).wrapping_mul(0x9E37);
                let mut local = 0u64;
                for seq in 0..OPS_PER_THREAD {
                    match next_op(&mut rng, thread, seq) {
                        Op::Insert(key, id) => {
                            index.write().unwrap().entry(key).or_default().insert(id);
                        }
                        Op::PrefixScan(prefix) => {
                            let guard = index.read().unwrap();
                            for (key, ids) in guard
                                .range::<[u8], _>((Bound::Included(&prefix[..]), Bound::Unbounded))
                            {
                                if !key.starts_with(&prefix) {
                                    break;
                                }
                                local += ids.len() as u64;
                            }
                        }
                        Op::Point(key) => {
                            let guard = index.read().unwrap();
                            local += guard.get(&key).map_or(0, BTreeSet::len) as u64;
                        }
                        Op::Remove(key, id) => {
                            let mut guard = index.write().unwrap();
                            if let Some(ids) = guard.get_mut(&key) {
                                ids.remove(&id);
                                if ids.is_empty() {
                                    guard.remove(&key);
                                }
                            }
                        }
                    }
                }
                sink.fetch_add(local, Relaxed);
            });
        }
    });
    std::hint::black_box(sink.load(Relaxed));
    (threads as u64 * OPS_PER_THREAD as u64) as f64 / start.elapsed().as_secs_f64()
}

/// Lock-free `SkipMap` over composite `(key, id)` entries — a concurrent ordered
/// multimap with no global lock; disjoint ops proceed in parallel.
fn run_skipmap(threads: usize) -> f64 {
    let index: SkipMap<(Vec<u8>, String), ()> = SkipMap::new();
    for prefix in 0..KEY_SPACE_PREFIXES {
        for customer in 0..CUSTOMERS_PER_PREFIX {
            index.insert(
                (
                    make_key(prefix, customer),
                    format!("seed-{prefix}-{customer}"),
                ),
                (),
            );
        }
    }
    let sink = AtomicU64::new(0);
    let start = Instant::now();
    std::thread::scope(|scope| {
        for thread in 0..threads {
            let index = &index;
            let sink = &sink;
            scope.spawn(move || {
                let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ (thread as u64).wrapping_mul(0x9E37);
                let mut local = 0u64;
                for seq in 0..OPS_PER_THREAD {
                    match next_op(&mut rng, thread, seq) {
                        Op::Insert(key, id) => {
                            index.insert((key, id), ());
                        }
                        Op::PrefixScan(prefix) => {
                            let lower = (prefix.clone(), String::new());
                            for entry in index.range(lower..) {
                                if !entry.key().0.starts_with(&prefix) {
                                    break;
                                }
                                local += 1;
                            }
                        }
                        Op::Point(key) => {
                            let lower = (key.clone(), String::new());
                            for entry in index.range(lower..) {
                                if entry.key().0 != key {
                                    break;
                                }
                                local += 1;
                            }
                        }
                        Op::Remove(key, id) => {
                            index.remove(&(key, id));
                        }
                    }
                }
                sink.fetch_add(local, Relaxed);
            });
        }
    });
    std::hint::black_box(sink.load(Relaxed));
    (threads as u64 * OPS_PER_THREAD as u64) as f64 / start.elapsed().as_secs_f64()
}

#[test]
#[ignore = "run on demand: cargo test --release --test index_concurrency_bench -- --ignored --nocapture"]
fn index_structure_concurrency_comparison() {
    println!(
        "\nM0 index concurrency bench — traced TPC-C mix, {} keys, {} ops/thread\n",
        KEY_SPACE_PREFIXES * CUSTOMERS_PER_PREFIX,
        OPS_PER_THREAD
    );
    println!(
        "{:<8} {:>16} {:>16} {:>10}",
        "threads", "RwLock<BTreeMap>", "SkipMap", "speedup"
    );
    for &threads in &[1usize, 2, 4, 8] {
        let btree = run_rwlock_btree(threads);
        let skip = run_skipmap(threads);
        println!(
            "{threads:<8} {:>13.0}/s {:>13.0}/s {:>9.2}x",
            btree,
            skip,
            skip / btree
        );
    }
    println!();
}
