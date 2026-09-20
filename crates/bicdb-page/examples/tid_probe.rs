//! Measures the cost of the read-through double descent: fetching a record by
//! key (`PagedStore::get` = key B-tree descent + heap read) versus fetching it
//! straight from the heap by `TupleLocator` (the PG-TID model a locator hint in
//! a secondary index entry would enable).
//!
//!   cargo run --release -p bicdb-page --example tid_probe -- [rows] [pool_mb]

use bicdb_page::{PagedStore, PagedStoreOptions};
use std::time::Instant;

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(300_000);
    let pool_mb: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(1024);

    let dir = std::env::temp_dir().join(format!("tid-probe-{rows}-{pool_mb}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let options = PagedStoreOptions::default()
        .with_buffer_pool_bytes(pool_mb * 1024 * 1024)
        .with_fsync(false);
    let (store, _report) = PagedStore::open(&dir, options).unwrap();

    // A record-shaped corpus: keys mimic `record_key(collection, pk)` layout,
    // values are ~256 bytes like a small TPC-C row.
    let value = vec![0x5a_u8; 256];
    let started = Instant::now();
    let mut i = 0usize;
    while i < rows {
        let xid = store.begin();
        let end = (i + 1000).min(rows);
        for row in i..end {
            let key = format!("c:orders:{row:012}");
            store.put(xid, key.as_bytes(), &value).unwrap();
        }
        store.commit(xid).unwrap();
        i = end;
    }
    println!("loaded {rows} rows in {:.1?}", started.elapsed());

    // Sample keys spread across the keyspace, and capture each chain-head
    // locator exactly as an index-entry hint would record it.
    let sample: Vec<(String, _)> = (0..rows)
        .step_by((rows / 20_000).max(1))
        .map(|row| {
            let key = format!("c:orders:{row:012}");
            let inspection = store.inspect_version_chain(key.as_bytes()).unwrap();
            let locator = inspection.samples[0].locator;
            (key, locator)
        })
        .collect();
    println!("sampled {} keys", sample.len());

    // Warm both paths once so the buffer pool state is comparable.
    for (key, locator) in &sample {
        assert!(store.get(key.as_bytes()).unwrap().is_some());
        assert!(!store.heap().get(*locator).unwrap().is_empty());
    }

    const ROUNDS: usize = 5;
    let mut descent_ns = Vec::new();
    let mut heap_ns = Vec::new();
    for _ in 0..ROUNDS {
        let t = Instant::now();
        let mut bytes = 0usize;
        for (key, _) in &sample {
            bytes += store.get(key.as_bytes()).unwrap().unwrap().len();
        }
        descent_ns.push(t.elapsed().as_nanos() as u64 / sample.len() as u64);
        std::hint::black_box(bytes);

        let t = Instant::now();
        let mut bytes = 0usize;
        for (_, locator) in &sample {
            bytes += store.heap().get(*locator).unwrap().len();
        }
        heap_ns.push(t.elapsed().as_nanos() as u64 / sample.len() as u64);
        std::hint::black_box(bytes);
    }
    descent_ns.sort_unstable();
    heap_ns.sort_unstable();
    let descent = descent_ns[ROUNDS / 2];
    let heap = heap_ns[ROUNDS / 2];
    println!("key descent (B-tree + heap): {descent} ns/op  {descent_ns:?}");
    println!("heap by locator (TID):       {heap} ns/op  {heap_ns:?}");
    println!(
        "descent overhead: {:.2}x ({} ns saved per fetch)",
        descent as f64 / heap as f64,
        descent.saturating_sub(heap)
    );

    let _ = std::fs::remove_dir_all(&dir);
}
